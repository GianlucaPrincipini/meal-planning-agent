# Usa un'immagine Python ufficiale
FROM python:3.12-slim

# Imposta la directory di lavoro nell'immagine
WORKDIR /app

# Copia file requirements (se presenti)
COPY requirements.txt .

# Installa dipendenze senza cache
RUN pip install --no-cache-dir -r requirements.txt

# Copia il resto del codice del backend
COPY . .

# Espone la porta (esempio: Flask usa 5000; FastAPI 8000)
EXPOSE 8000

# Comando di avvio (modifica secondo il tuo entrypoint Python)
CMD ["python", "main.py"]
