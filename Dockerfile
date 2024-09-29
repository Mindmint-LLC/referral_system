FROM python:3.12-slim

RUN apt-get update && apt-get install -y git

# Install Python dependencies.
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY run.sh .

CMD ["/bin/bash", "./run.sh"]