# Usa una versione LTS di Node
FROM node:18-alpine

# Imposta la directory di lavoro dentro l'immagine
WORKDIR /app

# Copia i file package.json / package-lock.json (se esistono)
COPY package*.json ./

# Installa le dipendenze
RUN npm install
RUN npm run dev

# Copia il resto dell'app nella WORKDIR
COPY . .

# Espone la porta su cui la tua app ascolta (es. 3000)
EXPOSE 3000

# Comando di avvio
CMD ["npm", "start"]
