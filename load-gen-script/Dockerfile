FROM python:3

LABEL maintainer="Gabe Weiss"

# Install dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy app into container
ENV APP_HOME /app
WORKDIR $APP_HOME
COPY . .

# Run the application
ENTRYPOINT [ "python", "run_load_generator.py" ]

