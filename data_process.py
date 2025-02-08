import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Any, Optional
import json
import os
from dataclasses import dataclass
import logging
from functools import lru_cache
import asyncio
from concurrent.futures import ThreadPoolExecutor
import io

logger = logging.getLogger(__name__)

@dataclass
class ProcessingResult:
    """Class to hold processing results"""
    original_rows: List[int]  # Row indices that were processed
    fixed_data: pd.DataFrame  # Only the rows that were fixed
    fixes: List[Dict[str, Any]]  # Details of changes made
    processing_stats: Dict[str, Any]

class DataStreamProcessor:
    def __init__(self, 
                 chunk_size: int = 1000, 
                 max_workers: int = 4,
                 rules_dir: str = "results/rules"):
        self.chunk_size = chunk_size
        self.max_workers = max_workers
        self.rules_dir = rules_dir
        self.dq_rules_cache = {}
        os.makedirs(rules_dir, exist_ok=True)
        
    def save_rules(self, file_id: str, rules: Dict):
        """Save DQ rules for a specific file"""
        rule_file = os.path.join(self.rules_dir, f"{file_id}_rules.json")
        with open(rule_file, 'w') as f:
            json.dump(rules, f, indent=2)
            
    def load_rules(self, file_id: str) -> Optional[Dict]:
        """Load previously generated rules"""
        rule_file = os.path.join(self.rules_dir, f"{file_id}_rules.json")
        if os.path.exists(rule_file):
            with open(rule_file, 'r') as f:
                return json.load(f)
        return None

    @lru_cache(maxsize=128)
    def get_cached_rules(self, column_name: str) -> Dict:
        """Cache and retrieve DQ rules for columns"""
        return self.dq_rules_cache.get(column_name, {})

    async def process_stream(self, 
                           file_content: str,
                           rules: Dict,
                           llm_processor) -> ProcessingResult:
        """Process data in streaming fashion"""
        fixes = []
        fixed_chunks = []
        processed_rows = []
        stats = {'total_rows': 0, 'chunks_processed': 0, 'fixes_applied': 0}

        # Create TextIO object for streaming
        buffer = io.StringIO(file_content)
        
        # Process chunks using pandas read_csv iterator
        chunk_iterator = pd.read_csv(buffer, chunksize=self.chunk_size)
        
        async def process_chunk(chunk: pd.DataFrame, chunk_index: int) -> Tuple[pd.DataFrame, List[Dict]]:
            chunk_fixes = []
            fixed_chunk = chunk.copy()
            chunk_modified = False

            for column, rule_set in rules.items():
                if column in fixed_chunk.columns:
                    # Apply fixes using LLM
                    column_result = await llm_processor.fix_column_data(
                        fixed_chunk[column],
                        rule_set['rules'],
                        rule_set['type']
                    )
                    
                    if column_result['changes']:
                        chunk_modified = True
                        fixed_chunk[column] = column_result['fixed_data']
                        for orig, fixed, rule in column_result['changes']:
                            row_idx = chunk_index * self.chunk_size + len(chunk_fixes)
                            chunk_fixes.append({
                                'column': column,
                                'row_index': row_idx,
                                'original_value': orig,
                                'fixed_value': fixed,
                                'rule_applied': rule
                            })

            return fixed_chunk if chunk_modified else None, chunk_fixes

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            chunk_futures = []
            
            for chunk_index, chunk in enumerate(chunk_iterator):
                stats['total_rows'] += len(chunk)
                stats['chunks_processed'] += 1
                
                # Process chunk
                future = asyncio.create_task(process_chunk(chunk, chunk_index))
                chunk_futures.append(future)
                
                # Process accumulated futures when we have enough
                if len(chunk_futures) >= self.max_workers:
                    completed_futures = await asyncio.gather(*chunk_futures)
                    for fixed_chunk, chunk_fixes in completed_futures:
                        if fixed_chunk is not None:
                            fixed_chunks.append(fixed_chunk)
                            fixes.extend(chunk_fixes)
                            processed_rows.extend(range(
                                len(processed_rows), 
                                len(processed_rows) + len(fixed_chunk)
                            ))
                    chunk_futures = []

            # Process remaining futures
            if chunk_futures:
                completed_futures = await asyncio.gather(*chunk_futures)
                for fixed_chunk, chunk_fixes in completed_futures:
                    if fixed_chunk is not None:
                        fixed_chunks.append(fixed_chunk)
                        fixes.extend(chunk_fixes)
                        processed_rows.extend(range(
                            len(processed_rows), 
                            len(processed_rows) + len(fixed_chunk)
                        ))

        stats['fixes_applied'] = len(fixes)
        
        # Combine fixed chunks if any exist
        fixed_df = pd.concat(fixed_chunks) if fixed_chunks else pd.DataFrame()
        
        return ProcessingResult(
            original_rows=processed_rows,
            fixed_data=fixed_df,
            fixes=fixes,
            processing_stats=stats
        )

    def save_results(self, 
                    result: ProcessingResult, 
                    file_id: str, 
                    output_dir: str = "results/processed"):
        """Save processing results"""
        os.makedirs(output_dir, exist_ok=True)
        
        # Save fixed data
        if not result.fixed_data.empty:
            result.fixed_data.to_csv(
                f"{output_dir}/{file_id}_fixed.csv", 
                index=False
            )
        
        # Save fixes and stats
        with open(f"{output_dir}/{file_id}_fixes.json", 'w') as f:
            json.dump({
                'processed_rows': result.original_rows,
                'fixes': result.fixes,
                'stats': result.processing_stats
            }, f, indent=2)