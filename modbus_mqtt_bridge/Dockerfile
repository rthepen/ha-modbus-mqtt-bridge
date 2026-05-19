FROM python:3.11-alpine

# Installeer systeemafhankelijkheden
RUN apk add --no-cache bash

# Werkdirectory instellen
WORKDIR /app

# Kopieer requirements en installeer python libraries
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Kopieer code
COPY bridge.py .
COPY config.yaml .
COPY run.sh .

# Maak run.sh uitvoerbaar
RUN chmod +x run.sh

# Start commando
CMD [ "./run.sh" ]
