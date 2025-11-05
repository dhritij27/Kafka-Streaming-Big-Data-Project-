from flask import Flask, render_template, jsonify
import pymysql
from flask_cors import CORS
import pandas as pd
import plotly.express as px
import plotly.utils
import json
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

app = Flask(__name__)
CORS(app)

# Database configuration
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'user': os.getenv('DB_USER', 'spark'),
    'password': os.getenv('DB_PASSWORD', 'spark123'),
    'database': os.getenv('DB_NAME', 'website_log'),
    'port': int(os.getenv('DB_PORT', 3306))
}

def get_db_connection():
    return pymysql.connect(**DB_CONFIG)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/dashboard')
def dashboard():
    try:
        conn = get_db_connection()
        
        # Get total PV
        with conn.cursor(pymysql.cursors.DictCursor) as cursor:
            cursor.execute("SELECT * FROM total_pv ORDER BY create_time DESC LIMIT 1")
            total_pv = cursor.fetchone() or {"total_pv": 0}
            
            # Get top IPs
            cursor.execute("SELECT ip, pv FROM ip_pv ORDER BY pv+0 DESC LIMIT 10")
            ip_data = cursor.fetchall()
            
            # Get search engine distribution
            cursor.execute("SELECT search_engine, pv FROM search_engine_pv ORDER BY pv+0 DESC")
            search_engine_data = cursor.fetchall()
            
            # Get top keywords
            cursor.execute("SELECT keyword, pv FROM keyword_pv WHERE keyword != '' ORDER BY pv+0 DESC LIMIT 10")
            keyword_data = cursor.fetchall()
            
            # Get user agent distribution
            cursor.execute("SELECT agent, pv FROM agent_pv ORDER BY pv+0 DESC LIMIT 10")
            agent_data = cursor.fetchall()
        
        # Create visualizations
        charts = {}
        
        # Search engine pie chart
        if search_engine_data:
            df = pd.DataFrame(search_engine_data)
            fig = px.pie(df, values='pv', names='search_engine', 
                         title='Search Engine Distribution',
                         hole=0.4)
            charts['search_engine_chart'] = json.loads(fig.to_json())
        
        # Top IPs bar chart
        if ip_data:
            df = pd.DataFrame(ip_data)
            fig = px.bar(df, x='ip', y='pv', 
                        title='Top IPs by Page Views',
                        labels={'ip': 'IP Address', 'pv': 'Page Views'})
            charts['ip_chart'] = json.loads(fig.to_json())
        
        # Keywords bar chart
        if keyword_data:
            df = pd.DataFrame(keyword_data)
            fig = px.bar(df, x='keyword', y='pv', 
                        title='Top Search Keywords',
                        labels={'keyword': 'Keyword', 'pv': 'Searches'})
            charts['keyword_chart'] = json.loads(fig.to_json())
        
        # User agent bar chart
        if agent_data:
            df = pd.DataFrame(agent_data)
            fig = px.bar(df, x='agent', y='pv', 
                        title='User Agent Distribution',
                        labels={'agent': 'User Agent', 'pv': 'Count'})
            charts['agent_chart'] = json.loads(fig.to_json())
        
        return jsonify({
            'success': True,
            'total_pv': total_pv.get('total_pv', 0),
            'last_updated': total_pv.get('update_time', ''),
            'charts': charts
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500
    finally:
        conn.close()

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
