const apiUrl = import.meta.env.VITE_API_URL || 'http://localhost:8000';

export const baseApiUrl = apiUrl.endsWith('/') ? apiUrl.slice(0, -1) : apiUrl;

export const getDomainRelations = async () => {
  try {
    const response = await fetch(`${baseApiUrl}/domain-relations`);
    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }
    return response.json();
  } catch (error) {
    console.error('Error fetching domain relations:', error);
    throw error;
  }
};
