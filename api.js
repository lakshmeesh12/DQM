import axios from 'axios';

const api = axios.create({
  baseURL: 'http://localhost:8000', // Base URL for the backend API
});

// Existing functions (unchanged, kept for reference)
export const fetchStorageOptions = async () => {
  try {
    const response = await api.get('/storage');
    return response.data;
  } catch (error) {
    console.error('Error fetching storage options:', error);
    throw error;
  }
};

export const fetchContainers = async (storageOption) => {
  try {
    const response = await api.get(`/storage/${storageOption}`);
    return response.data;
  } catch (error) {
    console.error('Error fetching containers:', error);
    throw error;
  }
};

export const fetchFileDetails = async (storageOption, container) => {
  try {
    const response = await api.get(`/storage/${storageOption}/${container}/files`);
    return response.data;
  } catch (error) {
    console.error('Error fetching file details:', error);
    throw error;
  }
};

export const fetchFileContent = async (storageOption, container, fileName) => {
  try {
    const response = await api.get(`/storage/${storageOption}/${container}/files/${fileName}`);
    return response.data;
  } catch (error) {
    console.error('Error fetching file content:', error);
    throw error;
  }
};

export const fetchLocalFiles = async () => {
  try {
    const response = await api.get('/storage/local/files');
    return response.data;
  } catch (error) {
    console.error('Error fetching local files:', error);
    throw error;
  }
};

export const uploadLocalFile = async (file) => {
  const formData = new FormData();
  formData.append('file', file);

  try {
    const response = await api.post('/storage/local/upload', formData, {
      headers: {
        'Content-Type': 'multipart/form-data',
      },
    });
    return response.data; // Returns {"filename": "unique_filename", ...}
  } catch (error) {
    console.error('Error uploading file:', error);
    throw error;
  }
};

export const generateFileRules = async (storageOption, container, fileName) => {
  try {
    const response = await api.post(`/storage/${storageOption}/${container}/${fileName}`);
    return response.data;
  } catch (error) {
    console.error('Error generating rules:', error);
    throw error;
  }
};

export const addRule = async (fileName, columnName, rule) => {
  try {
    const response = await api.post(
      `/rules/${fileName}/${columnName}/add-rule`,
      { rule }
    );
    return response.data;
  } catch (error) {
    console.error('Error adding rule:', error);
    throw error;
  }
};

export const deleteRule = async (fileName, columnName, ruleIndex) => {
  try {
    const response = await api.delete(
      `/rules/${fileName}/${columnName}/delete-rule/${ruleIndex}`
    );
    return response.data;
  } catch (error) {
    console.error('Error deleting rule:', error);
    throw error;
  }
};

export const fetchRulesFromDirectory = async (storageOption, container, fileName) => {
  try {
    const response = await api.get(`/rules/${storageOption}/${container}/${fileName}`);
    return response.data;
  } catch (error) {
    console.error('Error fetching rules from directory:', error);
    throw error;
  }
};

export const processAndExecuteQueries = async (selectedOption, containerName, fileName) => {
  try {
    const response = await api.post(
      `/process-and-execute-queries/${selectedOption}/${containerName}/${fileName}`,
      {},
      {
        headers: {
          'Content-Type': 'application/json',
        },
      }
    );
    return response.data.results;
  } catch (error) {
    throw new Error(error.response?.data?.detail || error.message || 'Error processing and executing queries');
  }
};

// api.js
export const validateData = async (containerName, fileName) => {
  try {
    const response = await api.post(
      `/validate-data/${containerName}/${fileName}`,
      {},
      {
        headers: {
          'Content-Type': 'application/json',
        },
      }
    );
    return response.data.results;
  } catch (error) {
    throw new Error(error.response?.data?.detail || error.message || 'Error validating data');
  }
};

// api.js (only adding the new function, rest remains unchanged)
export const uploadCorrectedDataset = async (provider, containerName, fileName, newFileName) => {
  try {
    const response = await api.post(
      `/upload-corrected/${provider}/${containerName}/${fileName}`,
      { new_file_name: newFileName },
      {
        headers: {
          'Content-Type': 'application/json',
        },
      }
    );
    return response.data;
  } catch (error) {
    throw new Error(error.response?.data?.detail || error.message || 'Error uploading corrected dataset');
  }
};
// export const executeStoredQueries = async (container_name, file_name) => {
//   try {
//     const response = await api.post(`/execute-queries/${container_name}/${file_name}`);
//     return response.data;
//   } catch (error) {
//     console.error('Error executing stored queries:', error);
//     throw error;
//   }
// };
