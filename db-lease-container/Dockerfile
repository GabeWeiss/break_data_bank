FROM python:3.8-slim

# So I can write to the log with Python easier
ENV PYTHONUNBUFFERED True

ENV APP_HOME /app
WORKDIR $APP_HOME

# Install dependencies
COPY requirements.txt /app/
RUN pip install -r requirements.txt

COPY . .

# Run the application
ENTRYPOINT hypercorn main:app --bind 0.0.0.0:${PORT}
