import os
import uuid
import logging
import numpy as np
from flask_cors import CORS
from flask import Flask, request, jsonify
from tasks import run_backtest, run_backtest_v2
from functools import wraps
from sqlalchemy import desc
from database.db import Session
from database.models import Backtest, Statistic, BenchmarkStatistic, SpyStatistic, AcwiStatistic
from utils import get_first_and_last_day
from google.cloud import bigquery
from thales.options_analyzer.monte_carlo.engine import MonteCarloEngine

logging.basicConfig(level=logging.INFO)

app = Flask(__name__)
CORS(app)
app.secret_key = os.getenv('FLASK_SECRET_KEY', "super-secret")

def require_api_key(view_function):
    @wraps(view_function)
    def decorated_function(*args, **kwargs):
        api_key = os.getenv('x-api-key')
        
        received_api_key = request.headers.get('x-api-key')
        if received_api_key and received_api_key == api_key:
            logging.info('API key validated successfully.')
            return view_function(*args, **kwargs)
        else:
            logging.warning('API key is missing or incorrect.')
            return jsonify(error="API key is missing or incorrect"), 403
    return decorated_function

@app.route('/start_backtest', methods=['POST'])
@require_api_key
def start_backtest():
    try:
        # Extract parameters from the request
        params = request.get_json()
        # Add backtest_id to params
        backtest_id = uuid.uuid4()
        params['backtest_id'] = backtest_id
        # Start the backtest task
        task = run_backtest_v2.delay(params)
        task_id = task.id
        # Save the initial backtesting info to Postgres
        pre_backtest_updates(task_id, params)
        # Return the task id to the client
        return jsonify({'task_id': task_id}), 202
    except Exception as e:
        return jsonify(error=str(e)), 400

@app.route('/backtests', methods=['GET'])
@require_api_key
def get_backtests():
    session = Session()
    try:
        # Perform a join between backtests and statistics tables and order by the submission date in descending order
        results = session.query(Backtest, Statistic, BenchmarkStatistic)\
            .outerjoin(Statistic, Backtest.id == Statistic.backtest_id)\
            .outerjoin(BenchmarkStatistic, Backtest.id == BenchmarkStatistic.backtest_id)\
            .order_by(desc(Backtest.submitted_at)).all()
        
        # Convert the query results to dictionaries and return as JSON
        backtests_statistics = [
            {
                'backtest': backtest.to_dict(),
                'statistic': statistic.to_dict() if statistic else None,
                'benchmark_statistic': benchmark_statistic.to_dict() if benchmark_statistic else None
            } for backtest, statistic, benchmark_statistic in results
        ]
        return jsonify(backtests_statistics)
    
    except Exception as e:
        logging.error(f'Failed to fetch backtests: {e}')
        return jsonify(error=str(e)), 400
    finally:
        session.close()

@app.route('/backtests', methods=['GET'])
@require_api_key
def get_backtests():
    session = Session()
    try:
        # Perform a join between backtests and statistics tables and order by the submission date in descending order
        results = session.query(Backtest, Statistic, SpyStatistic, AcwiStatistic)\
            .outerjoin(Statistic, Backtest.id == Statistic.backtest_id)\
            .outerjoin(SpyStatistic, Backtest.id == SpyStatistic.backtest_id)\
            .outerjoin(AcwiStatistic, Backtest.id == AcwiStatistic.backtest_id)\
            .order_by(desc(Backtest.submitted_at)).all()
        
        # Convert the query results to dictionaries and return as JSON
        backtests_statistics = [
            {
                'backtest': backtest.to_dict(),
                'statistic': statistic.to_dict() if statistic else None,
                'spy_statistic': spy_statistic.to_dict() if spy_statistic else None,
                'acwi_statistic': acwi_statistic.to_dict() if acwi_statistic else None
            } for backtest, statistic, spy_statistic, acwi_statistic in results
        ]
        return jsonify(backtests_statistics)
    
    except Exception as e:
        logging.error(f'Failed to fetch backtests: {e}')
        return jsonify(error=str(e)), 400
    finally:
        session.close()

@app.route('/backtest', methods=['GET'])
@require_api_key
def get_backtest():
    session = Session()
    try:
        backtest_id = request.args.get('backtest_id')
        if not backtest_id:
            return jsonify({"error": "backtest_id is required"}), 400

        logging.info(f"GET /backtest for {backtest_id}")

        # Perform a join between backtests and statistics tables and order by the submission date in descending order
        result = session.query(Backtest, Statistic, BenchmarkStatistic)\
            .outerjoin(Statistic, Backtest.id == Statistic.backtest_id)\
            .outerjoin(BenchmarkStatistic, Backtest.id == BenchmarkStatistic.backtest_id)\
            .filter(Backtest.id == backtest_id)\
            .first()
        
        if not result:
            return jsonify({"error": "Backtest not found"}), 404

        backtest, statistic, benchmark_statistic = result
        response = {
            'backtest': backtest.to_dict(),
            'statistic': statistic.to_dict() if statistic else None,
            'benchmark_statistic': benchmark_statistic.to_dict() if benchmark_statistic else None
        }
        
        return jsonify(response)
    
    except Exception as e:
        logging.error(f'Failed to fetch backtests: {e}')
        return jsonify(error=str(e)), 400
    finally:
        session.close()

@app.route('/monte_carlo', methods=['POST'])
@require_api_key
def monte_carlo():
    try:
        # Extract parameters from the request
        params = request.get_json()
        if not params:
            return jsonify({"error": "No JSON data provided"}), 400

        monte_carlo = MonteCarloEngine()
        result = monte_carlo.run(
            S0=params['S0'],
            K=params['K'],
            r=params['r'],
            sigma=params['sigma'],
            expiration_date=params['expiration_date'],
            num_simulations=params.get('num_simulations', 100000),
            option_type=params.get('option_type', 'put'),
            confidence_level=params.get('confidence_level', 0.95)
        )

        # Convert NumPy types to Python native types
        serializable_result = {
            'itm_probability': float(result['itm_probability']),
            'otm_probability': float(result['otm_probability']),
            'confidence_interval': (
                float(result['confidence_interval'][0]),
                float(result['confidence_interval'][1])
            ),
            'bs_delta': float(result['bs_delta']),
            # Convert only a subset of prices/paths for visualization
            'prices': result['prices'][:2500].tolist() if len(result['prices']) > 1000 else result['prices'].tolist(),
            'paths': result['paths'][:2500].tolist() if len(result['paths']) > 1000 else result['paths'].tolist(),
            'times': result['times'].tolist()
        }

        return jsonify(serializable_result)
    except Exception as e:
        return jsonify({"error": str(e)}), 400

@app.route('/data', methods=['GET'])
@require_api_key
def get_bigquery_data():
    try:
        # Extract parameters from the query string
        bigquery_table = request.args.get('bigquery_table')
        logging.info(f"GET /data for {bigquery_table}")

        # Create a BigQuery client
        client = bigquery.Client()

        # Query the table
        query = f"""
            SELECT tb.* 
            FROM `{bigquery_table}` tb
            ORDER BY tb.current_date ASC
            ;
        """

        query_job = client.query(query)
        results = query_job.result()
        results = [dict(row) for row in results]

        return jsonify(results)
    except Exception as e:
        return jsonify(error=str(e)), 400

@app.route('/raw_data', methods=['GET'])
@require_api_key
def get_raw_data():
    try:
        # Extract parameters from the query string
        bigquery_table = request.args.get('bigquery_table')
        logging.info(f"GET /raw_data for {bigquery_table}")

        # Create a BigQuery client
        client = bigquery.Client()

        # Query the table
        query = f"""
            WITH trade_legs AS (
                SELECT 
                    trade_id,
                    `current_date`,
                    'sell' AS leg_type,
                    'P' AS option_type,
                    root,
                    sell_strike AS strike,
                    expiration_date,
                    start_date,
                    daily_trade_pnl,
                    pnl,
                    position_size,
                    spread_price,
                    underlying_price
                FROM 
                    `{bigquery_table}`

                UNION ALL

                SELECT 
                    trade_id,
                    `current_date`,
                    'buy' AS leg_type,
                    'P' AS option_type,
                    root,
                    buy_strike AS strike,
                    expiration_date,
                    start_date,
                    daily_trade_pnl,
                    pnl,
                    position_size,
                    spread_price,
                    underlying_price
                FROM 
                    `{bigquery_table}`
            )
            SELECT 
                t.*,
                m.open,
                m.high,
                m.low,
                m.close,
                m.trade_volume,
                m.bid_size_1545,
                m.bid_1545,
                m.ask_size_1545,
                m.ask_1545,
                m.underlying_bid_1545,
                m.underlying_ask_1545,
                m.implied_underlying_price_1545,
                m.active_underlying_price_1545,
                m.implied_volatility_1545,
                m.delta_1545,
                m.gamma_1545,
                m.theta_1545,
                m.vega_1545,
                m.rho_1545,
                m.bid_size_eod,
                m.bid_eod,
                m.ask_size_eod,
                m.ask_eod,
                m.underlying_bid_eod,
                m.underlying_ask_eod,
                m.vwap,
                m.open_interest
            FROM 
                trade_legs t
            LEFT JOIN
                `manatechnologies-io.eod_options_chain_daily_dataset.*` m
            ON
                t.`current_date` = m.quote_date
                AND t.expiration_date = m.expiration
                AND t.strike = m.strike
                AND t.option_type = m.option_type
                AND t.root = m.root
            WHERE
                underlying_symbol = '^SPX'
            ORDER BY 
                `current_date` ASC,
                start_date,
                expiration_date
            ;
        """

        query_job = client.query(query)
        results = query_job.result()
        results = [dict(row) for row in results]

        return jsonify(results)
    except Exception as e:
        return jsonify(error=str(e)), 400
    
@app.route('/benchmark_data', methods=['GET'])
@require_api_key
def get_benchmark_data():
    try:
        # Extract parameters from the query string
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        logging.info(f"GET /benchmark_date from {start_date} to {end_date}")

        # Create a BigQuery client
        client = bigquery.Client()

        # Query the table
        query = f"""
            SELECT tb.*,
                SUM(tb.daily_return_percent) OVER (
                       ORDER BY tb.date ASC
                       ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                   ) as cumulative_return
            FROM `manatechnologies-io.benchmark.benchmark_alpha_vantage` tb
            WHERE tb.date >= '{start_date}' AND tb.date <= '{end_date}'
            ORDER BY tb.date ASC
            ;
        """

        query_job = client.query(query)
        results = query_job.result()
        results = [dict(row) for row in results]

        return jsonify(results)
    except Exception as e:
        return jsonify(error=str(e)), 400

def pre_backtest_updates(task_id, params):
    session = Session()
    try:
        logging.info(f'Saving task {task_id} to Postgres backtests table...')
        start_date, _ = get_first_and_last_day(params['start_date'])
        _, end_date = get_first_and_last_day(params['end_date'])
        strategy = params.get("strategy", "Percentage Under").lower().replace(" ", "_")
        if strategy == "percentage_under":
            strategy = "percent_under"
        new_backtest = Backtest(id=params['backtest_id'],
                                task_id=task_id,
                                start_date=start_date,
                                end_date=end_date,
                                spread=params.get('spread', 50),
                                initial_portfolio_value=params.get("initial_balance", 1000000),
                                status='running',
                                strategy=strategy,
                                strategy_unit=params.get("strategy_unit", 0.15))
        session.add(new_backtest)
        session.commit()
    except Exception as e:
        session.rollback()
        logging.error(f'Failed to save task {task_id} to Postgres: {e}')
        raise
    finally:
        session.close()
