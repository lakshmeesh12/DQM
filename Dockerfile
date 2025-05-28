# Multi-stage build for backend
FROM python:3.10-slim AS backend-base

WORKDIR /app

# Set PYTHONPATH to include /app/backend
ENV PYTHONPATH=/app/backend

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Upgrade pip
RUN pip install --upgrade pip

# Install Python dependencies
COPY backend/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy backend code
COPY backend/ /app/backend/

# Expose port
EXPOSE 8000

# Command to run the backend
CMD ["uvicorn", "backend.main:app", "--host", "0.0.0.0", "--port", "8000"]

# Multi-stage build for frontend
FROM node:18 AS frontend-build

WORKDIR /app/frontend

# Copy frontend files
COPY frontend/package.json frontend/package-lock.json ./
RUN npm install

COPY frontend/ ./
RUN npm run build

# Final stage for frontend (serve with nginx)
FROM nginx:alpine AS frontend

COPY --from=frontend-build /app/frontend/build /usr/share/nginx/html
COPY nginx.conf /etc/nginx/conf.d/default.conf

EXPOSE 80

CMD ["nginx", "-g", "daemon off;"]