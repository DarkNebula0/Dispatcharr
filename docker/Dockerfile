# Define base image build arguments (must be before any FROM)
ARG REPO_OWNER=dispatcharr
ARG REPO_NAME=dispatcharr
ARG BASE_TAG=base

# --- Build frontend ---

FROM node:24 AS frontend-builder

WORKDIR /app/frontend
COPY ./frontend /app/frontend
# remove any node_modules that may have been copied from the host (x86)
RUN rm -rf node_modules || true; \
    npm install --no-audit --progress=false;
RUN    npm run build; \
    rm -rf node_modules .cache

# --- Redeclare build arguments for the next stage ---
ARG REPO_OWNER
ARG REPO_NAME
ARG BASE_TAG

# --- Final image based on the dynamic base ---
FROM ghcr.io/${REPO_OWNER}/${REPO_NAME}:${BASE_TAG} AS final
ENV VIRTUAL_ENV=/dispatcharrpy
ENV PATH="$VIRTUAL_ENV/bin:$PATH"
WORKDIR /app

# Copy application code
COPY . /app
# Copy nginx configuration
COPY ./docker/nginx.conf /etc/nginx/sites-enabled/default
# Clean out existing frontend folder
RUN rm -rf /app/frontend
# Copy built frontend assets
COPY --from=frontend-builder /app/frontend/dist /app/frontend/dist

# Ensure drf-yasg is installed (fallback if not in base image)
RUN if [ -f /dispatcharrpy/bin/python ]; then \
    if ! /dispatcharrpy/bin/python -c "import drf_yasg" 2>/dev/null; then \
        echo "drf-yasg not found in base image, installing..." && \
        if ! /dispatcharrpy/bin/python -c "import pip" 2>/dev/null; then \
            echo "pip not found, bootstrapping pip using get-pip.py..." && \
            curl -sS https://bootstrap.pypa.io/get-pip.py -o /tmp/get-pip.py && \
            /dispatcharrpy/bin/python /tmp/get-pip.py --upgrade && \
            rm /tmp/get-pip.py; \
        fi && \
        /dispatcharrpy/bin/python -m pip install --no-cache-dir "drf-yasg>=1.21.0" && \
        /dispatcharrpy/bin/python -c "import drf_yasg; print(f'drf-yasg installed: {drf_yasg.__version__}')"; \
    else \
        echo "drf-yasg already installed in base image"; \
    fi; \
    else \
    echo "Error: Python virtual environment not found at /dispatcharrpy"; \
    exit 1; \
    fi

# Ensure guessit is installed (fallback if not in base image)
RUN if [ -f /dispatcharrpy/bin/python ]; then \
    if ! /dispatcharrpy/bin/python -c "import guessit" 2>/dev/null; then \
        echo "guessit not found in base image, installing..." && \
        if ! /dispatcharrpy/bin/python -c "import pip" 2>/dev/null; then \
            echo "pip not found, bootstrapping pip using get-pip.py..." && \
            curl -sS https://bootstrap.pypa.io/get-pip.py -o /tmp/get-pip.py && \
            /dispatcharrpy/bin/python /tmp/get-pip.py --upgrade && \
            rm /tmp/get-pip.py; \
        fi && \
        /dispatcharrpy/bin/python -m pip install --no-cache-dir guessit && \
        /dispatcharrpy/bin/python -c "import guessit; print(f'guessit installed: {guessit.__version__}')"; \
    else \
        echo "guessit already installed in base image"; \
    fi; \
    else \
    echo "Error: Python virtual environment not found at /dispatcharrpy"; \
    exit 1; \
    fi

# Add timestamp argument
ARG TIMESTAMP

# Update version.py with build timestamp if provided
RUN if [ -n "$TIMESTAMP" ]; then \
    echo "Updating timestamp to ${TIMESTAMP} in version.py" && \
    sed -i "s|__timestamp__ = None.*|__timestamp__ = '${TIMESTAMP}'    # Set during CI/CD build process|" /app/version.py && \
    cat /app/version.py; \
    fi

ENTRYPOINT ["/app/docker/entrypoint.sh"]