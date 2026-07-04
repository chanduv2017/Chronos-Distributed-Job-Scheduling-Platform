FROM node:20-alpine

WORKDIR /app

COPY package.json package-lock.json* ./
RUN npm ci --production

COPY src/ ./src/

# Run migrations then start the worker
CMD ["sh", "-c", "node src/db/migrate.js && node src/index.js"]
