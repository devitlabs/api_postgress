import re
from datetime import timedelta

from flask import Flask, jsonify, request
import psycopg2
from psycopg2 import Error
from flask_cors import CORS
from flask_jwt_extended import JWTManager, jwt_required, create_access_token, get_jwt_identity

from env import *

app = Flask(__name__)

CORS(app, origins=urls_allows)

app.config['JWT_SECRET_KEY'] = JWT_SECRET_KEY
app.config['JWT_ACCESS_TOKEN_EXPIRES'] = timedelta(hours=JWT_ACCESS_TOKEN_EXPIRES_IN_HOUR)
jwt = JWTManager(app)


def connect_to_database():
    try:
        # Connexion à la base de données
        connection = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        print("Connexion à la base de données PostgreSQL réussie")

        # Retourner l'objet de connexion
        return connection

    except Error as e:
        print(f"Erreur lors de la connexion à la base de données PostgreSQL: {e}")
        return None

def get_table_names(conn):
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = %s
        """, (DB_SCHEMA,))
        rows = cursor.fetchall()
        return [row[0] for row in rows]
    except psycopg2.Error as e:
        print("Erreur lors de la récupération des noms de table:", e)
        return []

def build_filter_condition(filtre):
    column_name, operator_and_value = filtre

    # Vérifiez si la valeur du filtre contient une virgule (pour la requête 'select')
    if ',' in operator_and_value:
        # Si c'est le cas, renvoyez simplement la colonne sans construire de condition
        return column_name

    operator, value = operator_and_value.split('.')

    operators = {
        'eq': '=',
        'gt': '>',
        'gte': '>=',
        'lt': '<',
        'lte': '<=',
        'neq': '<>',
        'in': 'IN',
        'is': '=',  # Modifier l'opérateur pour 'is' en '='
        'like': 'LIKE',
        'ilike': 'ILIKE'
    }

    # Traitement spécial pour les attributs booléens
    if operator == 'is' and value.lower() in ('true', 'false'):
        # Si la valeur est 'true' ou 'false', ajustez la valeur et l'opérateur
        value = value.lower() == 'true'
        return f"{column_name} = {value}"

    if operator in ('in', 'is'):
        value = f"({','.join(value.split(','))})"
    elif operator in ('like', 'ilike'):
        value = f"'{value.replace('*', '%')}'"
    else:
        value = f"'{value}'"

    return f"{column_name} {operators[operator]} {value}"


def build_ordering_conditions(order_params):
    columns = []
    for param in order_params.split(','):
        # Séparer la colonne et la direction (le cas échéant)
        parts = re.split(r'\.(asc|desc)$', param)
        column = parts[0]
        direction = 'ASC' if len(parts) == 1 else parts[1].upper()
        columns.append(f"{column} {direction}")
    return ', '.join(columns)
def build_logical_sql_conditions(conditions):
    sql_conditions = []
    for condition in conditions:
        # Supprimer les parenthèses
        condition = condition.strip('()')

        # Séparer l'attribut, l'opérateur et la valeur
        attribute, operator, value = condition.split('.')

        # Convertir l'opérateur en SQL
        operators = {
            'eq': '=',
            'gt': '>',
            'gte': '>=',
            'lt': '<',
            'lte': '<=',
            'neq': '<>',
            'in': 'IN',
            'is': '=',
            'like': 'LIKE',
            'ilike': 'ILIKE'
        }
        sql_operator = operators[operator]

        # Construire la condition SQL
        if operator == 'in':
            # Gérer le cas de l'opérateur 'in'
            value_list = value.split(',')
            sql_condition = f"{attribute} {sql_operator} ({','.join(value_list)})"
        elif operator == 'is':
            # Gérer le cas de l'opérateur 'is' pour les valeurs booléennes
            value = value.lower() == 'true'  # Convertir la chaîne en booléen
            sql_condition = f"{attribute} {sql_operator} {value}"
        else:
            # Gérer les autres opérateurs
            sql_condition = f"{attribute} {sql_operator} {value}"

        sql_conditions.append(sql_condition)

    return sql_conditions

def build_logical_condition(logical_operator, conditions):
    operators = {
        'or': ' OR ',
        'not': ' NOT ',
        'and': ' AND ',
        'not.and': ' AND NOT '  # Correction de l'opérateur 'not.and' en 'AND NOT'
    }

    # Traitement spécial pour l'opérateur 'not' seul
    if logical_operator == 'not' and len(conditions) == 1:
        return operators['not'] + conditions[0]

    # Vérifier si l'opérateur logique est dans le dictionnaire des opérateurs
    if logical_operator in operators:
        # Construire la condition logique en joignant les conditions avec l'opérateur approprié
        print(conditions) # ['(age.lt.18', 'age.gt.21)']
        sql_condition = build_logical_sql_conditions(conditions)
        print(sql_condition) # ["age > 18" ,"age > 21" ]
        return '(' + operators[logical_operator].join(sql_condition) + ')'
    else:
        # Si l'opérateur logique n'est pas dans le dictionnaire, renvoyer une chaîne vide
        return ""

def insert_one_data(table_name, data):
    conn = connect_to_database()
    if conn is None:
        return False, "Erreur de connexion à la base de données"

    try:
        cursor = conn.cursor()
        columns = ', '.join(data.keys())
        placeholders = ', '.join(['%s'] * len(data))
        query = f"INSERT INTO {DB_SCHEMA}.\"{table_name}\" ({columns}) VALUES ({placeholders})"
        cursor.execute(query, tuple(data.values()))
        conn.commit()
        conn.close()
        return True, "Donnée insérée avec succès"
    except psycopg2.Error as e:
        conn.rollback()
        print("Erreur lors de l'insertion de la donnée:", e)
        conn.close()
        return False, "Erreur lors de l'insertion de la donnée"

def insert_many_data(table_name, data):
    conn = connect_to_database()
    if conn is None:
        return False, "Erreur de connexion à la base de données"

    try:
        cursor = conn.cursor()
        columns = ', '.join(data[0].keys())  # Assume all dictionaries have the same keys
        placeholders = ', '.join(['%s'] * len(data[0]))
        query = f"INSERT INTO {DB_SCHEMA}.\"{table_name}\" ({columns}) VALUES ({placeholders})"
        cursor.executemany(query, [tuple(row.values()) for row in data])
        conn.commit()
        conn.close()
        return True, "Données insérées avec succès"
    except psycopg2.Error as e:
        conn.rollback()
        print("Erreur lors de l'insertion des données:", e)
        conn.close()
        return False, "Erreur lors de l'insertion des données"


def check_permission(permissions, required_permission):
    return required_permission in permissions


@app.route('/auth/login', methods=['POST'])
def login():
    username = request.json.get('username', None)
    password = request.json.get('password', None)

    if username == 'test' and password == 'password':
        role = roles["collaborateur"]
        access_token = create_access_token(identity={'username': username, 'role': role})
        return jsonify(access_token=access_token), 200
    else:
        return jsonify({"error": "Identifiants invalides"}), 401

@app.route('/db/<table_name>', methods=['GET'])
@jwt_required()
def get_table_data(table_name):

    current_role = get_jwt_identity()['role']
    if check_permission(current_role, f'read-{table_name}'):
        conn = connect_to_database()
        if conn is None:
            return jsonify({'error': 'Erreur de connexion à la base de données'}), 500

        try:
            if table_name not in get_table_names(conn):
                return jsonify({'error': 'Table non trouvée'}), 404

            cursor = conn.cursor()

            request_args = request.args.copy()

            limit_param = request_args.get('limit')
            limit = None
            if limit_param is not None:
                try:
                    limit = int(limit_param)
                    request_args.pop('limit', None)
                except ValueError:
                    return jsonify({'error': 'La valeur de limite doit être un entier valide'}), 400

            order_params = request_args.get('order')
            ordering_conditions = None
            if order_params:
                request_args.pop('order', None)
                ordering_conditions = build_ordering_conditions(order_params)

            select_params = request_args.get('select')
            if select_params:
                request_args.pop('select', None)
                query = f"SELECT {select_params} FROM {DB_SCHEMA}.\"{table_name}\""
            else:
                query = f"SELECT * FROM {DB_SCHEMA}.\"{table_name}\""

            filter_params = request_args

            if filter_params:
                conditions = []

                for key, value in filter_params.items():
                    if key == 'or' or key.startswith('not'):
                        logical_operator = key
                        conditions_list = value.split(',')
                        logical_condition = build_logical_condition(logical_operator, conditions_list)
                        conditions.append(logical_condition)
                    else:
                        conditions.append(build_filter_condition((key, value)))

                query += " WHERE " + " AND ".join(conditions)

            if ordering_conditions:
                query += " ORDER BY " + ordering_conditions

            if limit is not None:
                query += f" LIMIT {limit}"

            cursor.execute(query)
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            data = [dict(zip(columns, row)) for row in rows]
            conn.close()
            return jsonify({"total": len(data), "data": data})
        except psycopg2.Error as e:
            print("Erreur lors de l'exécution de la requête SELECT:", e)
            conn.close()
            return jsonify({'error': 'Erreur lors de l\'exécution de la requête SELECT'}), 500
    else:
        return jsonify({'error': 'Accès non autorisé'}), 403


@app.route('/db/<table_name>', methods=['POST'])
@jwt_required()
def insert_table_data(table_name):
    current_role = get_jwt_identity()['role']
    if check_permission(current_role, f'create-{table_name}'):
        if request.method == 'POST':
            data = request.json
            if isinstance(data, dict):  # Single insertion
                success, message = insert_one_data(table_name, data)
            elif isinstance(data, list):  # Multiple insertions
                success, message = insert_many_data(table_name, data)
            else:
                return jsonify({"success": False, "error": "Données d'entrée non valides"}), 400

            if success:
                return jsonify({"success": True, "message": message}), 201
            else:
                return jsonify({"success": False, "error": message}),
    else:
        return jsonify({'error': 'Accès non autorisé'}), 403

@app.route('/db/<table_name>', methods=['PATCH'])
@jwt_required()
def update_table_data(table_name):
    current_role = get_jwt_identity()['role']
    if check_permission(current_role, f'update-{table_name}'):
        conn = connect_to_database()
        if conn is None:
            return jsonify({'error': 'Erreur de connexion à la base de données'}), 500

        try:
            cursor = conn.cursor()

            # Extraire les données JSON de la requête
            update_data = request.get_json()

            # Construire la requête UPDATE de base
            query = f"UPDATE {DB_SCHEMA}.\"{table_name}\" SET "

            # Ajouter les colonnes à mettre à jour avec leurs nouvelles valeurs
            columns_to_update = [f"{column} = '{update_data[column]}'" for column in update_data]

            query += ", ".join(columns_to_update)

            # Ajouter des filtres horizontaux à la requête UPDATE
            filter_params = request.args

            if filter_params:
                conditions = []

                for key, value in filter_params.items():
                    conditions.append(build_filter_condition((key, value)))

                query += " WHERE " + " AND ".join(conditions)
            else :
                conn.close()
                return jsonify({'error': 'Erreur lors de l\'exécution de la requête UPDATE'}), 500
            # Exécuter la requête UPDATE
            cursor.execute(query)
            conn.commit()

            conn.close()

            return jsonify({'message': 'Données mises à jour avec succès'})
        except psycopg2.Error as e:
            print("Erreur lors de l'exécution de la requête UPDATE:", e)
            conn.close()
            return jsonify({'error': 'Erreur lors de l\'exécution de la requête UPDATE'}), 500
    else:
        return jsonify({'error': 'Accès non autorisé'}), 403

@app.route('/db/<table_name>', methods=['DELETE'])
@jwt_required()
def delete_table_data(table_name):
    current_role = get_jwt_identity()['role']
    if check_permission(current_role, f'delete-{table_name}'):
        conn = connect_to_database()
        if conn is None:
            return jsonify({'error': 'Erreur de connexion à la base de données'}), 500

        try:
            cursor = conn.cursor()

            # Construire la requête DELETE de base
            query = f"DELETE FROM {DB_SCHEMA}.\"{table_name}\""

            # Ajouter des filtres horizontaux à la requête DELETE
            filter_params = request.args
            if filter_params:
                conditions = []

                for key, value in filter_params.items():
                    if key == 'or' or key.startswith('not'):
                        logical_operator = key
                        conditions_list = value.split(',')
                        logical_condition = build_logical_condition(logical_operator, conditions_list)
                        conditions.append(logical_condition)
                    else:
                        conditions.append(build_filter_condition((key, value)))

                query += " WHERE " + " AND ".join(conditions)
            else :
                conn.close()
                return jsonify({'error': 'Erreur lors de l\'exécution de la requête DELETE'}), 500

            cursor.execute(query)
            conn.commit()

            conn.close()

            return jsonify({'message': 'Données supprimées avec succès'})
        except psycopg2.Error as e:
            print("Erreur lors de l'exécution de la requête DELETE:", e)
            conn.close()
            return jsonify({'error': 'Erreur lors de l\'exécution de la requête DELETE'}), 500

    else:
        return jsonify({'error': 'Accès non autorisé'}), 403



if __name__ == '__main__':
    app.run(debug=True)
