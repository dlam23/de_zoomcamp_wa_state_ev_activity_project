FROM mageai/mageai:latest

ARG PROJECT_NAME=my_de_zoomcamp_project
ARG MAGE_CODE_PATH=/home/mage_code
ARG USER_CODE_PATH=${MAGE_CODE_PATH}/${PROJECT_NAME}

# Install necessary tools
RUN apt-get update && apt-get install -y curl gnupg

# Add the Google Cloud SDK repository and key
RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && \
    curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg

# Install the Google Cloud SDK
RUN apt-get update && apt-get install google-cloud-sdk -y

WORKDIR ${MAGE_CODE_PATH}

# Replace [project_name] with the name of your project (e.g. demo_project)
COPY ${PROJECT_NAME} ${PROJECT_NAME}

# Set the USER_CODE_PATH variable to the path of user project.
# The project path needs to contain project name.
# Replace [project_name] with the name of your project (e.g. demo_project)
ENV USER_CODE_PATH=${USER_CODE_PATH}

# Install custom Python libraries
RUN pip3 install -r ${USER_CODE_PATH}/requirements.txt
# Install custom libraries within 3rd party libraries (e.g. dbt packages)
RUN python3 /app/install_other_dependencies.py --path ${USER_CODE_PATH}

ENV PYTHONPATH="${PYTHONPATH}:/home/mage_code"

CMD ["/bin/sh", "-c", "/app/run_app.sh"]
