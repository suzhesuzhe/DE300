# Use the latest Apache Spark image with Python support
FROM apache/spark-py:latest

# Run as root to install additional software
USER root

# Set the working directory to /tmp for any subsequent commands
WORKDIR /tmp

# Avoid warnings by switching to noninteractive
ENV DEBIAN_FRONTEND=noninteractive

# Update the package lists, install apt-utils and other necessary packages
RUN apt-get update && apt-get install -y \
    apt-utils \
    python3.10-venv \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

# Create a virtual environment for the project
RUN python3 -m venv demos

# Set environment variables to activate the virtual environment
ENV PATH="/tmp/demos/bin:$PATH"

# Install necessary Python packages separately
RUN pip install numpy
RUN pip install pyspark==3.4.0
RUN pip install jupyter
RUN pip install venv-pack

# Clean up unnecessary files and reset DEBIAN_FRONTEND
RUN apt-get clean && rm -rf /var/lib/apt/lists/* && \
    unset DEBIAN_FRONTEND

# Expose port 8888 for Jupyter Notebook
EXPOSE 8888

# Configure the container to run Jupyter Notebook, disable token authentication for ease of use in non-production environments
CMD ["jupyter", "notebook", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root", "--NotebookApp.token=''"]

