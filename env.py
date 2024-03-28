

# Configuration de la connexion à la base de données PostgreSQL
DB_HOST = 'localhost'  # Notez que le numéro de port est spécifié séparément
DB_PORT = 5432  # Le port par défaut de PostgreSQL est 5432
DB_NAME = 'postgres'
DB_USER = 'postgres'
DB_PASSWORD = 'password'
DB_SCHEMA = 'public'

roles = {
    'collaborateur': ['read-people', 'update-people'],
    'rh': ['read-people', 'create-people', 'update-people'],
    'admin': ['read-people', 'create-people', 'delete-people', 'update-people']
}

urls_allows=['http://localhost:1234', 'http://localhost']

JWT_SECRET_KEY = 'your-secret-key'

JWT_ACCESS_TOKEN_EXPIRES_IN_HOUR = 2