// At the top of App.js, add or update these imports
import React, { useState, useEffect, useRef  } from 'react'; // For React and hooks (useState, useEffect)
import { DragDropContext, Droppable, Draggable } from "react-beautiful-dnd"; // For drag-and-drop components
import { BrowserRouter as Router, Route, Routes, Navigate, Link, useNavigate, useLocation } from 'react-router-dom'; // For routing components and hooks
import {
  fetchStorageOptions,
  fetchContainers,
  fetchFileDetails,
  fetchFileContent,
  uploadLocalFile,
  fetchLocalFiles,
  generateFileRules,
  validateData,
  addRule, 
  deleteRule,
  fetchRulesFromDirectory,
  generateInvalidDataQueries,
  executeStoredQueries,
  uploadCorrectedDataset
} from './api/api'; // For API calls, including fetchRulesFromDirectory
import './styles/global.css'; // Import global styles
import './styles/HomePage.css'; // Import HomePage styles
import './styles/ContainersPage.css'; // Import ContainersPage styles
import './styles/LocalStoragePage.css'; // Import LocalStoragePage styles
import './styles/RulesPage.css'; // Import RulesPage styles
import './styles/ValidationPage.css';
import './styles/QueryResultsPage.css';
import './styles/ExecutionResultsPage.css';
import { useParams } from 'react-router-dom';
import LoadingOverlay from './api/LoadingOverlay';
import { 
  BarChart, 
  Bar, 
  XAxis, 
  YAxis, 
  Tooltip, 
  ResponsiveContainer, 
  LabelList 
} from 'recharts'; // For charts

// Header Component (unchanged)
function Header() {
  return (
    <div className="header">
      <nav className="home-link">
        <Link to="/">Home</Link>
      </nav>
      <div className="logo-container">
        <img
          src="/images/logo.png" // Relative path to public/images/logo.png
          alt="Company Logo"
          className="header-logo"
        />
      </div>
    </div>
  );
}
 

// Helper function to get the logo for each storage option (unchanged)
function getLogoForStorage(option) {
  switch (option.toLowerCase()) {
    case "azure":
      return "https://upload.wikimedia.org/wikipedia/commons/a/a8/Microsoft_Azure_Logo.svg";
    case "aws":
      return "https://upload.wikimedia.org/wikipedia/commons/9/93/Amazon_Web_Services_Logo.svg";
    case "local":
      return "https://cdn-icons-png.flaticon.com/512/484/484613.png";
    case "postgresql":
      return "https://upload.wikimedia.org/wikipedia/commons/2/29/Postgresql_elephant.svg";
    case "mysql":
      return "https://www.mysql.com/common/logos/logo-mysql-170x115.png";
    case "googledrive":
      return "https://upload.wikimedia.org/wikipedia/commons/d/da/Google_Drive_logo.png";
    case "snowflake":
      return "https://upload.wikimedia.org/wikipedia/commons/f/ff/Snowflake_Logo.svg";
    case "gcp":
        return "https://cdn.creazilla.com/icons/3253833/google-cloud-icon-sm.png";
    case "onedrive":
        return "https://auburn.service-now.com/onedrive_logo_icon-Recovered_med.pngx";
    default:
      console.warn(`No logo found for: ${option}`);
      return "";
  }
}
 
// Updated HomePage Component (simplified with debug logs)
function HomePage({
  setSelectedOption,
  setSelectedContainer,
  setSelectedFileForRules,
  setSelectedColumns,
  setLocalFiles,
  setSelectedFile
}) {
  const allStorageOptions = [
    "azure",
    "aws",
    "local",
    "postgresql",
    "mysql",
    "googledrive",
    "snowflake",
    "gcp",
    "onedrive"
  ];
  const functionalOptions = ["azure", "aws", "local"];
  const [selectedStorage, setSelectedStorage] = useState(null);
  const [isFlipping, setIsFlipping] = useState(false);
  const navigate = useNavigate();
 
  // Debug log to confirm options
  console.log('Rendering HomePage with options:', allStorageOptions);
 
  const handleStorageClick = (option) => {
    if (functionalOptions.includes(option)) {
      console.log(`Clicked functional option: ${option}`);
      setSelectedStorage(option);
      setSelectedOption(option);
      setIsFlipping(false);
    } else {
      console.log(`Clicked non-functional option: ${option} (no action)`);
    }
  };
 
  const handleRetrieveFiles = () => {
    if (selectedStorage && functionalOptions.includes(selectedStorage)) {
      console.log(`Retrieving files for: ${selectedStorage}`);
      setIsFlipping(true);
      setTimeout(() => {
        const logoUrl = getLogoForStorage(selectedStorage);
        if (logoUrl) {
          const proxiedUrl = `http://localhost:8000/proxy/image?url=${encodeURIComponent(logoUrl)}`;
          console.log('Storing proxied URL:', proxiedUrl);
          sessionStorage.setItem('flippedCardBackground', proxiedUrl);
        } else {
          console.error('No logo URL for:', selectedStorage);
        }
        navigate(`/${selectedStorage}`);
        setIsFlipping(false); // Reset after navigation
      }, 1500);
    }
  };
 
  return (
    <div className="home-page">
      <div className="storage-grid">
        {allStorageOptions.map((option, index) => {
          const isFunctional = functionalOptions.includes(option);
          console.log(`Rendering card: ${option} (functional: ${isFunctional})`);
          return (
            <div
              key={index}
              className={`storage-card ${isFunctional && selectedStorage === option && isFlipping ? 'flipping' : ''} ${!isFunctional ? 'display-only' : ''}`}
              onClick={() => handleStorageClick(option)}
            >
              <div className="card-inner">
                <div className="card-front">
                  <img
                    src={getLogoForStorage(option)}
                    alt={`${option} logo`}
                    className="storage-logo"
                    onError={() => console.error(`Failed to load logo for: ${option}`)}
                  />
                  <p className="storage-label">{option}</p>
                </div>
                {isFunctional && (
                  <div className="card-back">
                    <p className="storage-label-back">AI-Powered Storage</p>
                  </div>
                )}
              </div>
            </div>
          );
        })}
      </div>
      {selectedStorage && functionalOptions.includes(selectedStorage) && (
        <div className="actions">
          <button className="primary-button" onClick={handleRetrieveFiles}>
            {'Retrieve Files'}
          </button>
        </div>
      )}
    </div>
  );
}
function ContainersPage({ selectedOption, setSelectedContainer, setSelectedFileForRules, selectedFileForRules }) {
  const [containers, setContainers] = useState([]);
  const [files, setFiles] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [selectedContainerState, setSelectedContainerState] = useState(null);
  const [selectedFileState, setSelectedFileState] = useState(null);
  const [backgroundLoading, setBackgroundLoading] = useState(false);
  const [backgroundImage, setBackgroundImage] = useState(null);
  const navigate = useNavigate();

  const debounce = (func, wait) => {
    let timeout;
    return function executedFunction(...args) {
      const later = () => {
        clearTimeout(timeout);
        func(...args);
      };
      clearTimeout(timeout);
      timeout = setTimeout(later, wait);
    };
  };

  useEffect(() => {
    setLoading(true);
    fetchContainers(selectedOption)
      .then((data) => {
        setContainers(data.containers || []);
      })
      .catch((error) => {
        setError(error.message);
      })
      .finally(() => setLoading(false));
      
    setBackgroundImage(null);
    setSelectedContainerState(null);
    setSelectedFileState(null);
    
    const containerContent = document.querySelector('.container-content');
    if (containerContent) {
      containerContent.classList.remove('expanded', 'loading-glitch');
    }
  }, [selectedOption]);

  const loadContainerBackground = (container) => {
    setBackgroundLoading(true);
    const containerContent = document.querySelector('.container-content');
    if (containerContent) {
      containerContent.classList.add('loading-glitch');
    }
    
    setTimeout(() => {
      const logoUrl = sessionStorage.getItem('flippedCardBackground') || `/api/placeholder/400/300`;
      const img = new Image();
      img.crossOrigin = 'Anonymous';
      img.onload = () => {
        setBackgroundImage(img.src);
        setBackgroundLoading(false);
        if (containerContent) {
          containerContent.classList.remove('loading-glitch');
        }
      };
      img.onerror = () => {
        setBackgroundImage(logoUrl);
        setBackgroundLoading(false);
        if (containerContent) {
          containerContent.classList.remove('loading-glitch');
        }
      };
      img.src = logoUrl;
    }, 800);
  };

  const handleContainerCheckboxChange = (container) => {
    if (container !== selectedContainerState) {
      setSelectedContainerState(container);
      setFiles([]);
      setSelectedFileState(null);
      setSelectedFileForRules(null);
      loadContainerBackground(container);
      const containerContent = document.querySelector('.container-content');
      if (containerContent) {
        containerContent.classList.remove('expanded');
      }
      fetchFiles(container);
    } else {
      setSelectedContainerState(null);
      setFiles([]);
      setSelectedContainer(null);
      setBackgroundImage(null);
      const containerContent = document.querySelector('.container-content');
      if (containerContent) {
        containerContent.classList.remove('expanded');
      }
    }
  };

  const handleFileRadioChange = (file) => {
    setSelectedFileState(file); // Radio buttons allow only one selection
    setSelectedFileForRules(file);
  };

  const fetchFiles = async (container) => {
    setLoading(true);
    try {
      const data = await fetchFileDetails(selectedOption, container);
      setFiles(data.files || []);
      setTimeout(() => {
        const containerContent = document.querySelector('.container-content');
        if (containerContent) {
          containerContent.classList.add('expanded');
        }
      }, 300);
    } catch (error) {
      setError(error.message);
    } finally {
      setLoading(false);
    }
  };

  const handleGenerateRules = debounce(async () => {
    if (selectedContainerState && selectedFileState) {
      setLoading(true);
      try {
        const rules = await generateFileRules(selectedOption, selectedContainerState, selectedFileState);
        setSelectedFileForRules(selectedFileState);
        setSelectedContainer(selectedContainerState);
        navigate(`/${selectedOption}/container/${selectedContainerState}/file/${selectedFileState}/rules`, { 
          state: { 
            rules, 
            selectedOption, 
            selectedContainer: selectedContainerState,
            selectedFileForRules: selectedFileState
          },
          replace: true
        });
      } catch (error) {
        setError(error.message || 'Failed to generate rules');
        alert('Failed to generate rules. Please try again.');
      } finally {
        setLoading(false);
      }
    } else {
      alert('Please select a container and file before generating rules.');
    }
  }, 500);

  if (loading && !containers.length) {
    return <div className="loading">Loading...</div>;
  }

  if (error) {
    return <div className="error">Error: {error}</div>;
  }

  return (
    <div className={`containers-page ${backgroundLoading ? 'content-hidden' : 'loaded'}`}>
      <h2 className="text-3xl font-bold text-gray-800 mb-6">
        {selectedOption.charAt(0).toUpperCase() + selectedOption.slice(1)} Storage
      </h2>
      
      <div 
        className={`container-content ${files.length > 0 ? 'expanded' : ''} ${backgroundLoading ? 'loading-glitch' : ''}`}
        style={backgroundImage ? {
          backgroundImage: `url(${backgroundImage})`,
          backgroundSize: '200px auto',
          backgroundPosition: 'center',
          backgroundRepeat: 'no-repeat',
        } : {}}
      >
        <div className="bg-white p-4 rounded-xl shadow-lg mb-6">
          <h3 className="text-xl font-semibold text-gray-700 mb-4">Select Container</h3>
          <div className="container-grid grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
            {containers.map((container, index) => (
              <label 
                key={index} 
                className={`container-card ${selectedContainerState === container ? 'selected' : ''}`}
              >
                <input
                  type="checkbox"
                  checked={selectedContainerState === container}
                  onChange={() => handleContainerCheckboxChange(container)}
                  className="hidden"
                />
                <div className="flex items-center justify-between w-full">
                  <span className="text-lg font-medium truncate">
                    {container}
                  </span>
                </div>
              </label>
            ))}
          </div>
        </div>

        {selectedContainerState && (
          <div className="bg-white p-4 rounded-xl shadow-lg">
            <h3 className="text-xl font-semibold text-gray-700 mb-4">Select File</h3>
            <div className="container-grid grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
              {files.map((file, index) => (
                <label 
                  key={index} 
                  className={`file-card ${selectedFileState === file ? 'selected' : ''}`}
                >
                  <input
                    type="radio"
                    name="file-selection" // Ensures only one file can be selected
                    checked={selectedFileState === file}
                    onChange={() => handleFileRadioChange(file)}
                  />
                  <span className="text-lg font-medium truncate">
                    {file}
                  </span>
                </label>
              ))}
            </div>

            <button
              className={`primary-button mt-6 ${loading ? 'disabled' : ''}`}
              onClick={handleGenerateRules}
              disabled={!selectedFileState || loading}
            >
              {loading ? "Generating..." : "Generate AI Rules"}
            </button>
          </div>
        )}
      </div>
      
      {backgroundLoading && (
        <div className="background-loading-overlay">
          <div className="glitch-text">Loading Storage Data...</div>
        </div>
      )}
    </div>
  );
}

// Updated LocalStoragePage (fixed localFiles errors, removed unused variables)
function LocalStoragePage({ setSelectedContainer, setSelectedFileForRules, setLocalFiles, setSelectedFile, localFiles }) {
  const [selectedFileState, setSelectedFileState] = useState(null); // Defined selectedFileState as state
  const [uploadSuccess, setUploadSuccess] = useState(null);
  const [error, setError] = useState(null); // Removed setError warning by using it here if needed
  const [loading, setLoading] = useState(false);
  const [filesRetrieved, setFilesRetrieved] = useState(false);
  const [uploadedFile, setUploadedFile] = useState(null); // Track the most recently uploaded file
  const navigate = useNavigate();
  const location = useLocation(); // Import useLocation for debugging

  // Debounce function to prevent multiple rapid calls
  const debounce = (func, wait) => {
    let timeout;
    return function executedFunction(...args) {
      const later = () => {
        clearTimeout(timeout);
        func(...args);
      };
      clearTimeout(timeout);
      timeout = setTimeout(later, wait);
    };
  };

  const handleFileUpload = async (event) => {
    const file = event.target.files[0];
    if (file) {
      setLoading(true);
      try {
        const response = await uploadLocalFile(file); // Fetch response from backend
        const filename = response.filename || file.name; // Use filename from response (UUID)
        setUploadSuccess('File uploaded successfully!');
        setUploadedFile(filename); // Store the UUID filename from response
        setFilesRetrieved(false); // Reset to fetch files again
      } catch (error) {
        console.error('Upload error:', error); // Log error for debugging
        setError(error.message || 'Failed to upload file');
      } finally {
      setLoading(false);
      }
    }
  };

  const handleViewFiles = async () => {
    setLoading(true);
    try {
      const data = await fetchLocalFiles();
      setLocalFiles(data.files || []);
      setFilesRetrieved(true);
      setUploadedFile(null); // Clear uploaded file state after retrieving files
    } catch (error) {
      setError(error.message); // Use setError here to eliminate the warning
    } finally {
      setLoading(false);
    }
  };

  const handleFileSelectionForRules = (fileName) => {
    setSelectedFileForRules(fileName);
    setSelectedFile(fileName); // Update selectedFile for validation
    setSelectedContainer("local");
    navigate(`/local/file/${fileName}/rules`); // Navigate directly to rules page for the file
  };

  const handleGenerateRulesForUploadedFile = debounce(async () => {
    if (uploadedFile) {
      setLoading(true);
      try {
        console.log(`Calling generateFileRules for file: ${uploadedFile} with container: local, current location: ${location.pathname}`);
        const rules = await generateFileRules("local", "local", uploadedFile);
        
        // Set state before navigation
        setSelectedFileForRules(uploadedFile);
        setSelectedFile(uploadedFile);
        setSelectedContainer("local");
        
        // Log state for debugging
        console.log("State before navigation:", {
          selectedOption: "local",
          selectedContainer: "local",
          selectedFileForRules: uploadedFile,
          rulesGenerated: true
        });
        
        // Use the full path and ensure all required state is passed explicitly
        navigate(`/local/file/${uploadedFile}/rules`, { 
          state: { 
            rules: rules, 
            selectedOption: "local", 
            selectedContainer: "local",
            selectedFileForRules: uploadedFile
          },
          replace: true // Use replace to avoid browser back behavior issues
        });
        
        console.log(`Navigated to /local/file/${uploadedFile}/rules with state:`, {
          selectedOption: "local",
          selectedContainer: "local",
          selectedFileForRules: uploadedFile
        });
      } catch (error) {
        console.error('Error generating rules:', error);
        setError(error.message || 'Failed to generate rules');
        alert('Failed to generate rules. Please try again.');
      } finally {
        setLoading(false);
      }
    } else {
      alert('No file has been uploaded recently. Please upload a file first.');
    }
  }, 500); // Debounce with 500ms delay to prevent rapid calls

  if (loading) {
    return <div className="loading">Loading...</div>;
  }

  if (error) {
    return <div className="error">Error: {error}</div>;
  }

  return (
    <div className="local-storage-page">
      <h2>Local Storage</h2>
      
      <div className="file-upload">
        <h3>Upload a File</h3>
        <input type="file" onChange={handleFileUpload} />
        {uploadSuccess && <p className="success">{uploadSuccess}</p>}
      </div>

      <div className="actions">
        <button className="primary-button" onClick={handleViewFiles} disabled={loading}>
          {loading ? 'Retrieving...' : 'Retrieve Files'}
        </button>
        {uploadSuccess && uploadedFile && ( // Show Generate Rules button after successful upload
          <button 
            className="secondary-button"
            onClick={handleGenerateRulesForUploadedFile}
            style={{ marginLeft: '10px' }}
            disabled={loading}
          >
            {loading ? 'Generating...' : 'Generate Rules'}
          </button>
        )}
      </div>

      {filesRetrieved && localFiles && localFiles.length > 0 && (
        <div className="local-files">
          <h3>Local Files</h3>
          <ul>
            {localFiles.map((file, index) => (
              <li key={index}>
                <label>
                  <input
                    type="radio"
                    name="file"
                    value={file}
                    checked={selectedFileState === file}
                    onChange={() => setSelectedFileState(file)}
                  />
                  {file}
                </label>
                <button 
                  className="view-content-button" 
                  onClick={() => handleFileSelectionForRules(file)}
                >
                  View Content
                </button>
              </li>
            ))}
          </ul>
        </div>
      )}
      
      {filesRetrieved && (!localFiles || localFiles.length === 0) && (
        <div className="no-files">
          <p>No files found in local storage.</p>
        </div>
      )}
    </div>
  );
}
// Updated RulesPage component with improved UI
function RulesPage({ selectedOption, selectedContainer, selectedFileForRules, selectedColumns, setSelectedColumns }) {
  const [generatedRules, setGeneratedRules] = useState(null);
  const [loading, setLoading] = useState(true);
  const [statisticsColumns, setStatisticsColumns] = useState([]);
  const [hoveredRule, setHoveredRule] = useState(null);
  const [showAddRuleInput, setShowAddRuleInput] = useState(null);
  const [newRuleText, setNewRuleText] = useState('');
  const [queryLoading, setQueryLoading] = useState(false);
  const navigate = useNavigate();
  const location = useLocation();

  useEffect(() => {
    let mounted = true;

    const loadRules = async () => {
      if (selectedOption && selectedContainer && selectedFileForRules) {
        setLoading(true);
        try {
          console.log('RulesPage loading with state:', location.state, 'and props:', {
            selectedOption,
            selectedContainer,
            selectedFileForRules
          });
          const stateRules = location.state?.rules;
          if (stateRules) {
            if (mounted) {
              setGeneratedRules(stateRules);
              setStatisticsColumns(stateRules.details?.sanitized_columns || []);
            }
          } else {
            console.log(`Fetching rules from API for ${selectedOption}/${selectedContainer}/${selectedFileForRules}`);
            const rules = await generateFileRules(selectedOption, selectedContainer, selectedFileForRules);
            if (!rules || !rules.details || !rules.details.sanitized_columns) {
              throw new Error("Invalid response: missing details or sanitized_columns");
            }
            if (mounted) {
              setGeneratedRules(rules);
              setStatisticsColumns(rules.details.sanitized_columns);
            }
          }
        } catch (error) {
          console.error('Error loading rules:', error);
          alert('Failed to load rules. Please try again.');
        } finally {
          if (mounted) setLoading(false);
        }
      } else {
        console.warn('Missing required parameters, staying on current page or going back');
        if (mounted) setLoading(false);
        navigate(-1, { replace: true });
      }
    };

    loadRules();

    return () => {
      mounted = false;
    };
  }, [selectedOption, selectedContainer, selectedFileForRules, location.state, navigate]);

  const handleGenerateQueries = async () => {
    if (!selectedOption || !selectedContainer || !selectedFileForRules) {
      alert("Missing required parameters for query generation.");
      return;
    }
    setQueryLoading(true);
    try {
      const results = await generateInvalidDataQueries(selectedOption, selectedContainer, selectedFileForRules);
      navigate(`/${selectedOption}/container/${selectedContainer}/file/${selectedFileForRules}/query-results`, {
        state: { queryResults: results }
      });
    } catch (error) {
      console.error("Query generation error:", error);
      alert(error.message || "Failed to generate invalid data queries");
    } finally {
      setQueryLoading(false);
    }
  };

  const handleAddRule = async (column) => {
    if (!newRuleText.trim()) {
      alert('Please enter a rule before saving.');
      return;
    }
    try {
      setLoading(true);
      const baseFileName = selectedFileForRules.split('.')[0].toLowerCase();
      const response = await addRule(baseFileName, column, newRuleText);
      const updatedRules = await fetchRulesFromDirectory(selectedOption, selectedContainer, selectedFileForRules);
      if (!updatedRules.rules) throw new Error("Invalid response: missing rules");
      const transformedRules = {
        rules: {},
        statistics: {},
        details: { sanitized_columns: Object.keys(updatedRules.rules) }
      };
      transformedRules.details.sanitized_columns.forEach(col => {
        transformedRules.rules[col] = {
          rules: updatedRules.rules[col].rules,
          type: updatedRules.rules[col].type || 'text',
          dq_dimensions: updatedRules.rules[col].dq_dimensions || ["Validity", "Completeness", "Relevance"]
        };
        transformedRules.statistics[col] = {
          completeness: updatedRules.rules[col].statistics?.completeness || 100.0,
          unique_count: updatedRules.rules[col].statistics?.unique_count || 0,
          uniqueness_ratio: updatedRules.rules[col].statistics?.uniqueness_ratio || 100.0,
          non_empty_ratio: updatedRules.rules[col].statistics?.non_empty_ratio || 100.0
        };
      });
      setGeneratedRules(transformedRules);
      setStatisticsColumns(transformedRules.details.sanitized_columns);
      setShowAddRuleInput(null);
      setNewRuleText('');
    } catch (error) {
      console.error('Error adding rule:', error);
      alert('Failed to add rule. Please try again.');
    } finally {
      setLoading(false);
    }
  };

  const handleDeleteRule = async (column, ruleIndex) => {
    if (!window.confirm('Are you sure you want to delete this rule?')) return;
    try {
      setLoading(true);
      const baseFileName = selectedFileForRules.split('.')[0].toLowerCase();
      const response = await deleteRule(baseFileName, column, ruleIndex);
      const updatedRules = await fetchRulesFromDirectory(selectedOption, selectedContainer, selectedFileForRules);
      if (!updatedRules.rules) throw new Error("Invalid response: missing rules");
      const transformedRules = {
        rules: {},
        statistics: {},
        details: { sanitized_columns: Object.keys(updatedRules.rules) }
      };
      transformedRules.details.sanitized_columns.forEach(col => {
        transformedRules.rules[col] = {
          rules: updatedRules.rules[col].rules,
          type: updatedRules.rules[col].type,
          dq_dimensions: updatedRules.rules[col].dq_dimensions
        };
        transformedRules.statistics[col] = {
          completeness: updatedRules.rules[col].statistics.completeness,
          unique_count: updatedRules.rules[col].statistics.unique_count,
          uniqueness_ratio: updatedRules.rules[col].statistics.uniqueness_ratio,
          non_empty_ratio: updatedRules.rules[col].statistics.non_empty_ratio
        };
      });
      setGeneratedRules(transformedRules);
      setStatisticsColumns(transformedRules.details.sanitized_columns);
    } catch (error) {
      console.error('Error deleting rule:', error);
      alert('Failed to delete rule. Please try again.');
    } finally {
      setLoading(false);
    }
  };

  const handleColumnCheckboxChange = (column) => {
    setSelectedColumns((prevColumns) =>
      prevColumns.includes(column)
        ? prevColumns.filter((col) => col !== column)
        : [...prevColumns, column]
    );
  };

  const onDragEnd = (result) => {
    if (!result.destination) return;
    const items = Array.from(statisticsColumns);
    const [reorderedItem] = items.splice(result.source.index, 1);
    items.splice(result.destination.index, 0, reorderedItem);
    setStatisticsColumns(items);
  };

  const formatChartValue = (value, name) => {
    if (name === "Unique Count") return Math.round(value).toLocaleString();
    return value.toFixed(1) + '%';
  };

  if (loading) return <div className="loading">Loading...</div>;
  if (!generatedRules) return <div className="no-results">No rules could be generated.</div>;

  return (
    <div className="rules-page">
      <h2 className="rules-title">Generated Rules for {selectedFileForRules}</h2>
      <div className="rules-container">
        <div className="statistics-sidebar">
          <h3>Column Statistics</h3>
          <div className="statistics-scrollable">
            <DragDropContext onDragEnd={onDragEnd}>
              <Droppable droppableId="statistics" direction="vertical">
                {(provided) => (
                  <div
                    className="statistics-list"
                    {...provided.droppableProps}
                    ref={provided.innerRef}
                  >
                    {statisticsColumns.map((column, index) => {
                      const columnData = generatedRules.statistics[column];
                      const statsData = [
                        { name: "Completeness", value: columnData.completeness },
                        { name: "Unique Count", value: columnData.unique_count },
                        { name: "Uniqueness", value: columnData.uniqueness_ratio },
                        { name: "Non-Empty", value: columnData.non_empty_ratio },
                      ];
                      return (
                        <Draggable key={column} draggableId={column} index={index}>
                          {(provided) => (
                            <div
                              className="column-statistics"
                              ref={provided.innerRef}
                              {...provided.draggableProps}
                              {...provided.dragHandleProps}
                            >
                              <h4>{column}</h4>
                              <ResponsiveContainer width="100%" height={120}>
                                <BarChart
                                  layout="vertical"
                                  data={statsData}
                                  margin={{ left: 10, right: 40, top: 10, bottom: 5 }}
                                >
                                  <XAxis type="number" tickFormatter={(value) => value} />
                                  <YAxis type="category" dataKey="name" width={80} tick={{ fontSize: 11 }} />
                                  <Tooltip
                                    formatter={formatChartValue}
                                    labelStyle={{ fontWeight: 'bold', color: '#333' }}
                                    contentStyle={{
                                      backgroundColor: 'white',
                                      border: '1px solid #ddd',
                                      borderRadius: '4px',
                                      boxShadow: '0 2px 4px rgba(0,0,0,0.15)'
                                    }}
                                  />
                                  <Bar
                                    dataKey="value"
                                    fill="#6a11cb"
                                    barSize={16}
                                    radius={[0, 4, 4, 0]}
                                    animationDuration={500}
                                  >
                                    <LabelList
                                      dataKey="value"
                                      position="right"
                                      formatter={formatChartValue}
                                      style={{ fill: '#333', fontWeight: 'bold', fontSize: '12px' }}
                                    />
                                  </Bar>
                                </BarChart>
                              </ResponsiveContainer>
                            </div>
                          )}
                        </Draggable>
                      );
                    })}
                    {provided.placeholder}
                  </div>
                )}
              </Droppable>
            </DragDropContext>
          </div>
        </div>

        <div className="generated-rules-section">
          <h3>Rules</h3>
          <div className="generated-rules scrollable-section">
            <table>
              <thead>
                <tr>
                  <th>Column</th>
                  <th>Rules</th>
                  <th>Type</th>
                  <th>Dimensions</th>
                </tr>
              </thead>
              <tbody>
                {statisticsColumns.map((column) => {
                  const columnData = generatedRules.rules[column];
                  return (
                    <tr key={column}>
                      <td>
                        <div className="column-checkbox">
                          <input
                            type="checkbox"
                            onChange={() => handleColumnCheckboxChange(column)}
                            checked={selectedColumns.includes(column)}
                            id={`checkbox-${column}`}
                          />
                          <label htmlFor={`checkbox-${column}`}>{column}</label>
                        </div>
                      </td>
                      <td>
                        <ul className="rules-list">
                          {columnData.rules.map((rule, index) => (
                            <li
                              key={index}
                              className="rule-item"
                              onMouseEnter={() => setHoveredRule(`${column}-${index}`)}
                              onMouseLeave={() => setHoveredRule(null)}
                            >
                              <span className="rule-text">{rule}</span>
                              {hoveredRule === `${column}-${index}` && (
                                <button
                                  className="delete-rule-btn"
                                  onClick={() => handleDeleteRule(column, index)}
                                  title="Delete rule"
                                >
                                  <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                                    <polyline points="3 6 5 6 21 6"></polyline>
                                    <path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"></path>
                                    <line x1="10" y1="11" x2="10" y2="17"></line>
                                    <line x1="14" y1="11" x2="14" y2="17"></line>
                                  </svg>
                                </button>
                              )}
                            </li>
                          ))}
                          <li className="add-rule-item">
                            {showAddRuleInput === column ? (
                              <div className="rule-editor">
                                <textarea
                                  value={newRuleText}
                                  onChange={(e) => setNewRuleText(e.target.value)}
                                  placeholder="Enter a new rule in natural language (e.g., 'This column should always start with capital letters')"
                                  rows={3}
                                />
                                <div className="rule-editor-buttons">
                                  <button
                                    className="save-rule-btn"
                                    onClick={() => handleAddRule(column)}
                                    disabled={loading}
                                  >
                                    {loading ? 'Saving...' : 'Save'}
                                  </button>
                                  <button
                                    className="cancel-rule-btn"
                                    onClick={() => {
                                      setShowAddRuleInput(null);
                                      setNewRuleText('');
                                    }}
                                    disabled={loading}
                                  >
                                    Cancel
                                  </button>
                                </div>
                              </div>
                            ) : (
                              <button
                                className="add-rule-btn"
                                onClick={() => setShowAddRuleInput(column)}
                                title="Add new rule"
                              >
                                <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                                  <line x1="12" y1="5" x2="12" y2="19"></line>
                                  <line x1="5" y1="12" x2="19" y2="12"></line>
                                </svg>
                                <span>Add Rule</span>
                              </button>
                            )}
                          </li>
                        </ul>
                      </td>
                      <td>{columnData.type}</td>
                      <td>
                        <ul className="dimensions-list">
                          {columnData.dq_dimensions.map((dim, index) => (
                            <li key={index}>{dim}</li>
                          ))}
                        </ul>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>

          <div className="actions">
            <button
              className="primary-button generate-sql-button"
              onClick={handleGenerateQueries}
              disabled={queryLoading}
            >
              {queryLoading ? 'Generating Queries...' : 'Generate Queries'}
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}

function QueryResultsPage() {
  const location = useLocation();
  const navigate = useNavigate();
  const queryResults = location.state?.queryResults;

  const pathSegments = location.pathname.split('/');
  const fileName = pathSegments[pathSegments.length - 2]; // Second-to-last segment
  const containerName = pathSegments[pathSegments.length - 4]; // Fourth-to-last segment

  const handleBack = () => {
    navigate(-1); // Go back to RulesPage
  };

  const handleExecuteQueries = async () => {
    try {
      const response = await executeStoredQueries(containerName, fileName);
      navigate(`/${pathSegments[1]}/container/${containerName}/file/${fileName}/execution-results`, {
        state: { executionResponse: response }
      });
    } catch (error) {
      console.error('Execution error:', error);
      navigate(`/${pathSegments[1]}/container/${containerName}/file/${fileName}/execution-results`, {
        state: { executionResponse: { error: error.message || 'Failed to execute queries' } }
      });
    }
  };

  if (!queryResults) {
    return (
      <div className="query-results-page">
        <div className="no-results-container">
          <p className="no-results">No query results available.</p>
          <button className="back-button" onClick={handleBack}>
            <svg
              xmlns="http://www.w3.org/2000/svg"
              width="20"
              height="20"
              viewBox="0 0 24 24"
              fill="none"
              stroke="currentColor"
              strokeWidth="2"
              strokeLinecap="round"
              strokeLinejoin="round"
            >
              <path d="M15 18l-6-6 6-6" />
            </svg>
            Back to Rules
          </button>
        </div>
      </div>
    );
  }

  return (
    <div className="query-results-page">
      <header className="page-header">
        <h2 className="page-title">Query Results for {fileName}</h2>
        <button className="back-button" onClick={handleBack}>
          <svg
            xmlns="http://www.w3.org/2000/svg"
            width="20"
            height="20"
            viewBox="0 0 24 24"
            fill="none"
            stroke="currentColor"
            strokeWidth="2"
            strokeLinecap="round"
            strokeLinejoin="round"
          >
            <path d="M15 18l-6-6 6-6" />
          </svg>
          Back to Rules
        </button>
      </header>

      <div className="results-container">
        <section className="query-section successful-queries">
          <h3>Successful Queries</h3>
          {queryResults.successful_queries && Object.keys(queryResults.successful_queries).length > 0 ? (
            <div className="queries-list">
              {Object.entries(queryResults.successful_queries).map(([column, query]) => (
                <div key={column} className="query-card">
                  <h4 className="query-column">{column}</h4>
                  <pre className="sql-code">{formatSQLQuery(query)}</pre>
                </div>
              ))}
            </div>
          ) : (
            <p className="no-data">No successful queries generated.</p>
          )}
        </section>

        {queryResults.failed_queries && Object.keys(queryResults.failed_queries).length > 0 && (
          <section className="query-section failed-queries">
            <h3>Failed Queries</h3>
            <div className="queries-list">
              {Object.entries(queryResults.failed_queries).map(([column, error]) => (
                <div key={column} className="query-card error-card">
                  <h4 className="query-column">{column}</h4>
                  <pre className="error-message">{error}</pre>
                </div>
              ))}
            </div>
          </section>
        )}

        <div className="execution-actions">
          <button className="execute-button" onClick={handleExecuteQueries}>
            Execute Queries
          </button>
        </div>
      </div>
    </div>
  );
}

function formatSQLQuery(query) {
  return query
    .replace(/\s+/g, ' ')
    .replace(/SELECT\s*\*/gi, 'SELECT *')
    .replace(/FROM/gi, '\nFROM')
    .replace(/WHERE/gi, '\nWHERE')
    .replace(/OR/gi, '\n   OR')
    .replace(/AND/gi, '\n   AND')
    .replace(/NOT\s+IN/gi, 'NOT IN')
    .replace(/GROUP\s+BY/gi, '\nGROUP BY')
    .replace(/HAVING/gi, '\nHAVING')
    .replace(/CAST\s*\(/gi, 'CAST(')
    .replace(/\s*\(\s*/g, ' (')
    .replace(/\s*\)\s*/g, ')')
    .replace(/,\s*/g, ', ')
    .replace(/;/g, ';\n')
    .trim();
}

function ExecutionResultsPage() {
  const location = useLocation();
  const navigate = useNavigate();
  const executionResponse = location.state?.executionResponse;
  const [showRuleEditor, setShowRuleEditor] = useState(false);
  const [newRuleText, setNewRuleText] = useState('');
  const [selectedColumn, setSelectedColumn] = useState('');
  const [ruleLoading, setRuleLoading] = useState(false);
  const [isLoading, setIsLoading] = useState(false); // New state for loading overlay
  const [filters, setFilters] = useState({
    surrogate_key: '',
    invalid_column: '',
    invalid_value: '',
  });

  const pathSegments = location.pathname.split('/');
  const fileName = pathSegments[pathSegments.length - 2];
  const containerName = pathSegments[pathSegments.length - 4];

  const handleBack = () => {
    navigate(-1);
  };

  const handleAddRuleClick = () => {
    setShowRuleEditor(true);
  };

  const handleSaveRule = async () => {
    if (!newRuleText.trim() || !selectedColumn) {
      alert('Please select a column and enter a rule.');
      return;
    }

    setRuleLoading(true);
    try {
      const baseFileName = fileName.split('.')[0].toLowerCase();
      await addRule(baseFileName, selectedColumn, newRuleText);
      alert('Rule added successfully!');
      setNewRuleText('');
      setSelectedColumn('');
      setShowRuleEditor(false);
    } catch (error) {
      console.error('Error adding rule:', error);
      alert(error.message || 'Failed to add rule.');
    } finally {
      setRuleLoading(false);
    }
  };

  const handleCancelRule = () => {
    setNewRuleText('');
    setSelectedColumn('');
    setShowRuleEditor(false);
  };

  const handleCorrectInvalidRecords = async () => {
    setIsLoading(true); // Show loading overlay
    
    try {
      // Simulate a delay to show the loading state
      await new Promise(resolve => setTimeout(resolve, 3000));
      
      const response = await validateData(containerName, fileName);
      setIsLoading(false); // Hide loading overlay
      navigate(`/${pathSegments[1]}/container/${containerName}/file/${fileName}/validation-results`, {
        state: { validationResponse: response }
      });
    } catch (error) {
      console.error('Validation error:', error);
      setIsLoading(false); // Hide loading overlay
      navigate(`/${pathSegments[1]}/container/${containerName}/file/${fileName}/validation-results`, {
        state: { validationResponse: { error: error.message || 'Failed to validate data' } }
      });
    }
  };

  const handleFilterChange = (e, column) => {
    setFilters({
      ...filters,
      [column]: e.target.value.toLowerCase(),
    });
  };

  if (!executionResponse) {
    return (
      <div className="execution-results-page">
        <div className="no-results-container">
          <p className="no-results">No execution results available.</p>
          <button className="back-button" onClick={handleBack}>
            Back to Query Results
          </button>
        </div>
      </div>
    );
  }

  const invalidData = executionResponse.results?.invalid_data || [];
  const uniqueColumns = [...new Set(invalidData.map(item => item.invalid_column))];

  // Filter the data based on user input
  const filteredData = invalidData.filter(row =>
    String(row.surrogate_key).toLowerCase().includes(filters.surrogate_key) &&
    String(row.invalid_column).toLowerCase().includes(filters.invalid_column) &&
    String(row.invalid_value).toLowerCase().includes(filters.invalid_value)
  );

  return (
    <div className="execution-results-page">
      {/* Loading Overlay */}
      <LoadingOverlay isVisible={isLoading} />
      
      <header className="page-header">
        <div className="header-content">
          <h2 className="page-title">Execution Results</h2>
          <div className="header-actions">
            <button className="correct-button" onClick={handleCorrectInvalidRecords}>
              Correct Invalid Records
            </button>
            <button className="back-button" onClick={handleBack}>
              <svg
                xmlns="http://www.w3.org/2000/svg"
                width="20"
                height="20"
                viewBox="0 0 24 24"
                fill="none"
                stroke="currentColor"
                strokeWidth="2"
                strokeLinecap="round"
                strokeLinejoin="round"
              >
                <path d="M15 18l-6-6 6-6" />
              </svg>
              Back to Query Results
            </button>
          </div>
        </div>
      </header>
      <div className="results-container">
        <section className="execution-section">
          <h3>Invalid Data</h3>
          {invalidData.length > 0 ? (
            <div className="invalid-data-container">
              <div className="add-rule-section">
                <p className="add-rule-prompt">Do you want to add any column-specific rule?</p>
                {!showRuleEditor ? (
                  <button className="add-rule-button" onClick={handleAddRuleClick}>
                    Add Rule
                  </button>
                ) : (
                  <div className="rule-editor">
                    <select
                      value={selectedColumn}
                      onChange={(e) => setSelectedColumn(e.target.value)}
                      className="column-select"
                    >
                      <option value="">Select a column</option>
                      {uniqueColumns.map((col) => (
                        <option key={col} value={col}>
                          {col}
                        </option>
                      ))}
                    </select>
                    <textarea
                      value={newRuleText}
                      onChange={(e) => setNewRuleText(e.target.value)}
                      placeholder="Enter a new rule (e.g., 'This column should always start with capital letters')"
                      rows={3}
                      className="rule-textarea"
                    />
                    <div className="rule-editor-buttons">
                      <button
                        className="save-rule-button"
                        onClick={handleSaveRule}
                        disabled={ruleLoading}
                      >
                        {ruleLoading ? 'Saving...' : 'Save'}
                      </button>
                      <button
                        className="cancel-rule-button"
                        onClick={handleCancelRule}
                        disabled={ruleLoading}
                      >
                        Cancel
                      </button>
                    </div>
                  </div>
                )}
              </div>
              <div className="invalid-data-table-wrapper">
                <table className="invalid-data-table">
                  <thead>
                    <tr>
                      <th>Surrogate Key</th>
                      <th>Invalid Column</th>
                      <th>Invalid Value</th>
                    </tr>
                    <tr className="filter-row">
                      <td>
                        <input
                          type="text"
                          value={filters.surrogate_key}
                          onChange={(e) => handleFilterChange(e, 'surrogate_key')}
                          placeholder="Filter..."
                          className="filter-input"
                        />
                      </td>
                      <td>
                        <input
                          type="text"
                          value={filters.invalid_column}
                          onChange={(e) => handleFilterChange(e, 'invalid_column')}
                          placeholder="Filter..."
                          className="filter-input"
                        />
                      </td>
                      <td>
                        <input
                          type="text"
                          value={filters.invalid_value}
                          onChange={(e) => handleFilterChange(e, 'invalid_value')}
                          placeholder="Filter..."
                          className="filter-input"
                        />
                      </td>
                    </tr>
                  </thead>
                  <tbody>
                    {filteredData.map((row, index) => (
                      <tr key={index}>
                        <td>{row.surrogate_key}</td>
                        <td>{row.invalid_column}</td>
                        <td>{row.invalid_value}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          ) : (
            <p className="no-data">No invalid data found.</p>
          )}
        </section>
      </div>
    </div>
  );
}

function ValidationResultsPage() {
  const location = useLocation();
  const navigate = useNavigate();
  const validationResponse = location.state?.validationResponse;
  const [showUploadEditor, setShowUploadEditor] = useState(false);
  const [newFileName, setNewFileName] = useState('');
  const [uploadLoading, setUploadLoading] = useState(false);

  const pathSegments = location.pathname.split('/');
  const provider = pathSegments[1]; // e.g., "aws" or "azure"
  const containerName = pathSegments[pathSegments.length - 4];
  const fileName = pathSegments[pathSegments.length - 2];

  const handleBack = () => {
    navigate(-1);
  };

  const handleDownloadCSV = () => {
    const correctedDataset = validationResponse.corrected_dataset || [];
    if (correctedDataset.length === 0) {
      alert('No corrected dataset available to download.');
      return;
    }

    const headers = Object.keys(correctedDataset[0]).join(',');
    const rows = correctedDataset.map(row => 
      Object.values(row).map(value => `"${value}"`).join(',')
    ).join('\n');
    const csvContent = `${headers}\n${rows}`;

    const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
    const link = document.createElement('a');
    link.href = URL.createObjectURL(blob);
    link.download = 'corrected_dataset.csv';
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
  };

  const handleUploadBackToSource = async () => {
    if (!newFileName.trim()) {
      alert('Please enter a filename.');
      return;
    }

    setUploadLoading(true);
    try {
      const response = await uploadCorrectedDataset(provider, containerName, fileName, newFileName);
      alert(response.message);
      setNewFileName('');
      setShowUploadEditor(false);
    } catch (error) {
      console.error('Upload error:', error);
      alert(error.message || 'Failed to upload corrected dataset.');
    } finally {
      setUploadLoading(false);
    }
  };

  if (!validationResponse) {
    return (
      <div className="validation-results-page">
        <div className="no-results-container">
          <p className="no-results">No validation results available.</p>
          <button className="back-button" onClick={handleBack}>
            Back to Execution Results
          </button>
        </div>
      </div>
    );
  }

  const successfulValidation = validationResponse.successful_validation || {};
  const correctedDataset = validationResponse.corrected_dataset || [];

  return (
    <div className="validation-results-page">
      <header className="page-header">
        <div className="header-content">
          <h2 className="page-title">Validation Results</h2>
          <div className="header-actions">
            <button className="download-button" onClick={handleDownloadCSV}>
              Download to CSV
            </button>
            <button className="upload-source-button" onClick={() => setShowUploadEditor(true)}>
              Upload Back to Source
            </button>
            {showUploadEditor && (
              <div className="upload-editor">
                <input
                  type="text"
                  value={newFileName}
                  onChange={(e) => setNewFileName(e.target.value)}
                  placeholder="Enter filename (e.g., corrected_data)"
                  className="filename-input"
                  disabled={uploadLoading}
                />
                <button
                  className="upload-button"
                  onClick={handleUploadBackToSource}
                  disabled={uploadLoading}
                >
                  {uploadLoading ? 'Uploading...' : 'Upload'}
                </button>
              </div>
            )}
            <button className="back-button" onClick={handleBack}>
              <svg
                xmlns="http://www.w3.org/2000/svg"
                width="20"
                height="20"
                viewBox="0 0 24 24"
                fill="none"
                stroke="currentColor"
                strokeWidth="2"
                strokeLinecap="round"
                strokeLinejoin="round"
              >
                <path d="M15 18l-6-6 6-6" />
              </svg>
              Back to Execution Results
            </button>
          </div>
        </div>
      </header>
      <div className="results-container">
        <section className="corrected-dataset-section">
          <h3>Corrected Dataset</h3>
          {correctedDataset.length > 0 ? (
            <div className="corrected-dataset-table">
              <table>
                <thead>
                  <tr>
                    {Object.keys(correctedDataset[0]).map((key) => (
                      <th key={key}>{key}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {correctedDataset.map((row, index) => (
                    <tr key={index}>
                      {Object.values(row).map((value, i) => (
                        <td key={i}>{value}</td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : (
            <p className="no-data">No corrected dataset available.</p>
          )}
        </section>

        <section className="validation-section">
          <h3>Successful Validations</h3>
          {Object.keys(successfulValidation).length > 0 ? (
            Object.entries(successfulValidation).map(([column, data]) => (
              <div key={column} className="column-validation">
                <h4 className="column-title">{column}</h4>
                <div className="validation-table">
                  <table>
                    <thead>
                      <tr>
                        <th>Surrogate Key</th>
                        <th>Invalid Value</th>
                        <th>Corrected Value</th>
                        <th>Reason</th>
                      </tr>
                    </thead>
                    <tbody>
                      {data.records.map((record, index) => (
                        <tr key={index}>
                          <td>{record.surrogate_key}</td>
                          <td>{record.invalid_value}</td>
                          <td>{record.corrected_value}</td>
                          <td>{record.reason}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
                <div className="validation-summary">
                  <p>Invalid Values: {data.summary.invalid_value_count}</p>
                  <p>Corrected: {data.summary.corrected_count}</p>
                </div>
              </div>
            ))
          ) : (
            <p className="no-data">No successful validations found.</p>
          )}
        </section>
      </div>
    </div>
  );
}

// Main App Component
function App() {
  const [selectedOption, setSelectedOption] = useState('');
  const [selectedContainer, setSelectedContainer] = useState('');
  const [selectedFileForRules, setSelectedFileForRules] = useState(null);
  const [selectedColumns, setSelectedColumns] = useState([]);
  const [localFiles, setLocalFiles] = useState([]);
  const [selectedFile, setSelectedFile] = useState(null);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    setLoading(false);
  }, []);

  if (loading) {
    return <div className="loading">Loading...</div>;
  }

  return (
    <Router>
      <div className="app">
        <Header />
        <div className="content">
          <Routes>
            <Route
              path="/"
              element={
                <HomePage
                  setSelectedOption={setSelectedOption}
                  setSelectedContainer={setSelectedContainer}
                  setSelectedFileForRules={setSelectedFileForRules}
                  setSelectedColumns={setSelectedColumns}
                  setLocalFiles={setLocalFiles}
                  setSelectedFile={setSelectedFile}
                />
              }
            />
            <Route
              path="/:provider"
              element={
                <ContainersPage
                  selectedOption={selectedOption}
                  setSelectedContainer={setSelectedContainer}
                  setSelectedFileForRules={setSelectedFileForRules}
                  selectedFileForRules={selectedFileForRules}
                />
              }
            />
            <Route
              path="/local"
              element={
                <LocalStoragePage
                  setSelectedContainer={setSelectedContainer}
                  setSelectedFileForRules={setSelectedFileForRules}
                  setLocalFiles={setLocalFiles}
                  setSelectedFile={setSelectedFile}
                  localFiles={localFiles}
                />
              }
            />
            <Route
              path="/:provider/container/:containerName"
              element={<Navigate to={`/${selectedOption}`} replace />}
            />
            <Route
              path="/:provider/container/:containerName/file/:fileName/rules"
              element={
                <RulesPage
                  selectedOption={selectedOption}
                  selectedContainer={selectedContainer}
                  selectedFileForRules={selectedFileForRules}
                  selectedColumns={selectedColumns}
                  setSelectedColumns={setSelectedColumns}
                />
              }
            />
            <Route
              path="/:provider/container/:containerName/file/:fileName/query-results"
              element={<QueryResultsPage />}
            />
            <Route
              path="/:provider/container/:containerName/file/:fileName/execution-results"
              element={<ExecutionResultsPage />}
            />
            <Route
              path="/:provider/container/:containerName/file/:fileName/validation-results"
              element={<ValidationResultsPage />}
            />
            <Route path="*" element={<Navigate to="/" replace />} />
          </Routes>
        </div>
      </div>
    </Router>
  );
}

export default App;