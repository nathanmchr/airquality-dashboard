# Étape 1 : Image Python légère
FROM python:3.13-slim

# Étape 2 : Répertoire de travail
WORKDIR /app

# Étape 3 : Installer les dépendances
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Étape 4 : Copier tout le backend (API + ETL)
COPY . .

# Étape 5 : Exposer le port de FastAPI
EXPOSE 8000

# Étape 6 : Lancer FastAPI (tu peux changer la commande si tu veux un script custom)
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]