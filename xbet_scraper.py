import time
import random
import json
import pandas as pd
import os
import signal
import sys
import traceback
import requests
import glob
import threading
import gc
from datetime import datetime, timedelta
from http.server import HTTPServer, BaseHTTPRequestHandler
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import re
import logging
from logging.handlers import RotatingFileHandler

# Set up logging with rotation
log_dir = os.environ.get('DATA_DIR', 'data')
os.makedirs(log_dir, exist_ok=True)

log_handler = RotatingFileHandler(
    os.path.join(log_dir, 'scraper.log'),
    maxBytes=1024*1024,  # 1MB max file size
    backupCount=3
)
logging.basicConfig(
    handlers=[log_handler],
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Global scraper reference for the web server
global_scraper = None

class SimpleHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()
        
        # Show basic status
        last_update = "Never"
        live_count = 0
        upcoming_count = 0
        
        if global_scraper and global_scraper.last_successful_run:
            last_update = global_scraper.last_successful_run.strftime("%Y-%m-%d %H:%M:%S")
            live_count = len(global_scraper.live_events)
            upcoming_count = len(global_scraper.upcoming_events)
        
        status_html = f"""
        <html>
        <head>
            <title>1xbet Scraper Status</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 0; padding: 20px; line-height: 1.6; }}
                h1 {{ color: #333; }}
                .container {{ max-width: 800px; margin: 0 auto; }}
                .status-ok {{ color: green; }}
                .status-error {{ color: red; }}
                .stats {{ background-color: #f5f5f5; padding: 15px; border-radius: 5px; margin-top: 20px; }}
            </style>
        </head>
        <body>
            <div class="container">
                <h1>1xbet Scraper Status</h1>
                <p>Status: <span class="status-ok">Running</span></p>
                <p>Last update: {last_update}</p>
                
                <div class="stats">
                    <h2>Current Stats</h2>
                    <p>Live events: {live_count}</p>
                    <p>Upcoming events: {upcoming_count}</p>
                    <p>Server Time: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</p>
                </div>
            </div>
        </body>
        </html>
        """
        self.wfile.write(status_html.encode())
    
    def log_message(self, format, *args):
        # Suppress logging of HTTP requests to reduce noise
        return

def run_server(port=10000):
    """Run a simple HTTP server to keep the service alive and report status"""
    server_address = ('', port)
    httpd = HTTPServer(server_address, SimpleHandler)
    logging.info(f"Starting web server on port {port}")
    print(f"Starting web server on port {port}")
    httpd.serve_forever()

class XbetScraper:
    def __init__(self):
        self.base_url = "https://ind.1xbet.com/"
        self.update_interval = 120  # 2 minutes between updates to conserve resources
        self.running = True
        self.last_update = None
        self.data_dir = os.environ.get('DATA_DIR', 'data')
        
        # Create data directory if it doesn't exist
        os.makedirs(self.data_dir, exist_ok=True)
        
        # Setup Chrome options - optimized for Render environment
        self.chrome_options = Options()
        self.chrome_options.add_argument("--headless")
        self.chrome_options.add_argument("--no-sandbox")
        self.chrome_options.add_argument("--disable-dev-shm-usage")
        self.chrome_options.add_argument("--disable-gpu")
        self.chrome_options.add_argument("--window-size=1280,720")  # Reduced window size
        self.chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
        
        # Memory optimization
        self.chrome_options.add_argument("--disable-extensions")
        self.chrome_options.add_argument("--disable-speech-api")
        self.chrome_options.add_argument("--disable-component-extensions-with-background-pages")
        self.chrome_options.add_argument("--blink-settings=imagesEnabled=false")  # Disable images for lower memory usage
        self.chrome_options.add_argument("--js-flags=--expose-gc")  # Enable JS garbage collection
        self.chrome_options.add_argument("--disable-software-rasterizer")
        self.chrome_options.add_argument("--disable-dev-tools")
        self.chrome_options.add_argument("--disable-notifications")
        self.chrome_options.add_argument("--disable-popup-blocking")
        
        # Initialize WebDriver with ChromeDriverManager - adapted for Render
        logging.info("Setting up Chrome WebDriver...")
        print("Setting up Chrome WebDriver...")
        try:
            # Try to use ChromeDriverManager (works in local dev)
            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=self.chrome_options)
        except Exception as e:
            logging.error(f"Error initializing ChromeDriver with ChromeDriverManager: {e}")
            print(f"Error initializing ChromeDriver with ChromeDriverManager: {e}")
            print("Trying alternative method for deployed environment...")
            try:
                # Try direct Chrome initialization (for Render)
                self.driver = webdriver.Chrome(options=self.chrome_options)
            except Exception as e2:
                logging.error(f"Second method also failed: {e2}")
                print(f"Second method also failed: {e2}")
                # If both methods fail, we'll continue without initializing and try later
                self.driver = None
                
        if self.driver:
            self.wait = WebDriverWait(self.driver, 20)  # Increased wait time for reliability
            logging.info("WebDriver initialized successfully")
            print("WebDriver initialized successfully")
        
        # Data storage
        self.live_events = []
        self.upcoming_events = []
        self.leagues = []
        
        # Setup signal handler for clean termination
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        
        # Health check state
        self.last_successful_run = None
        self.driver_start_time = datetime.now()
        
    def signal_handler(self, sig, frame):
        """Handle termination signals to exit cleanly"""
        logging.info(f"Received termination signal ({sig}). Shutting down gracefully...")
        print(f"\nReceived termination signal ({sig}). Shutting down gracefully...")
        self.running = False
    
    def __del__(self):
        """Close the browser when done"""
        if hasattr(self, 'driver') and self.driver:
            try:
                self.driver.quit()
                logging.info("WebDriver closed successfully")
                print("WebDriver closed successfully")
            except:
                logging.error("Error closing WebDriver")
                print("Error closing WebDriver")
    
    def initialize_driver(self):
        """Try to initialize the driver if it failed earlier or died"""
        if not self.driver:
            try:
                logging.info("Attempting to initialize WebDriver...")
                print("Attempting to initialize WebDriver...")
                try:
                    service = Service(ChromeDriverManager().install())
                    self.driver = webdriver.Chrome(service=service, options=self.chrome_options)
                except:
                    self.driver = webdriver.Chrome(options=self.chrome_options)
                    
                self.wait = WebDriverWait(self.driver, 20)
                self.driver_start_time = datetime.now()
                logging.info("WebDriver initialized successfully")
                print("WebDriver initialized successfully")
                return True
            except Exception as e:
                logging.error(f"Failed to initialize WebDriver: {e}")
                print(f"Failed to initialize WebDriver: {e}")
                # If we can't initialize the driver, we'll try again later
                return False
        return True
    
    def get_page_content(self, url=None):
        """Fetch the page content with Selenium and wait for it to load"""
        # Initialize driver if needed
        if not self.initialize_driver():
            logging.error("Cannot get page content - driver initialization failed")
            print("Cannot get page content - driver initialization failed")
            return None
            
        try:
            target_url = url if url else self.base_url
            logging.info(f"Fetching page: {target_url}")
            print(f"Fetching page: {target_url}")
            self.driver.get(target_url)
            
            # Wait for the content to load with increased timeout
            try:
                # Wait for main event containers
                self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".c-events__item")))
                print("Main content elements loaded")
                
                # Wait specifically for odds elements to load
                self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".c-bets__bet")))
                print("Odds elements loaded")
            except Exception as e:
                logging.warning(f"Timed out waiting for elements: {e}")
                print(f"Warning: Timed out waiting for elements: {e}")
                print("Will try to continue anyway")
            
            # Simplified scrolling - just enough to load content
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight*0.5);")
            time.sleep(2)
            
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)
            
            logging.info("Page loaded successfully")
            print("Page loaded successfully")
            self.last_successful_run = datetime.now()
            return self.driver.page_source
        except Exception as e:
            logging.error(f"Error fetching page: {e}")
            print(f"Error fetching page: {e}")
            # Try to recover by reinitializing the driver
            try:
                self.driver.quit()
            except:
                pass
            self.driver = None
            return None
    
    def parse_live_events(self, html_content):
        """Parse the live events section of the page"""
        soup = BeautifulSoup(html_content, 'html.parser')
        live_events = []
        
        # Find the container for live events - looking for the LIVE Bets section
        live_container = soup.select_one('div[id="line_bets_on_main"].c-events.greenBack')
        if not live_container:
            logging.warning("Live events container not found")
            print("Live events container not found")
            return live_events
            
        # Find all live events containers - these are the league sections
        live_sections = live_container.select('.dashboard-champ-content')
        logging.info(f"Found {len(live_sections)} live sections")
        print(f"Found {len(live_sections)} live sections")
        
        for section_index, section in enumerate(live_sections):
            # Get league info from the header
            league_header = section.select_one('.c-events__item_head')
            if not league_header:
                print(f"No header found for section {section_index}")
                continue
                
            # Get sport type
            sport_icon = league_header.select_one('.icon use')
            sport_type = sport_icon['xlink:href'].split('#')[-1].replace('sports_', '') if sport_icon else "Unknown"
            sport_name = self.get_sport_name(sport_type)
            
            # Get country
            country_element = league_header.select_one('.flag-icon use')
            country = country_element['xlink:href'].split('#')[-1] if country_element else "International"
            
            # Get league name
            league_name_element = league_header.select_one('.c-events__liga')
            league_name = league_name_element.text.strip() if league_name_element else "Unknown League"
            league_url = league_name_element['href'] if league_name_element and 'href' in league_name_element.attrs else ""
            
            print(f"Processing league: {league_name} ({sport_name})")
            
            # Get the available bet types for this league
            bet_types = []
            bet_title_elements = league_header.select('.c-bets__title')
            for title_elem in bet_title_elements:
                bet_types.append(title_elem.text.strip())
            
            # Get all matches in this league
            matches = section.select('.c-events__item_col .c-events__item_game')
            print(f"Found {len(matches)} matches in {league_name}")
            
            for match_index, match in enumerate(matches):
                match_data = {
                    'sport': sport_name,
                    'country': country,
                    'league': league_name,
                    'league_url': league_url,
                    'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
                
                # Get team names
                teams_container = match.select_one('.c-events__teams')
                if teams_container:
                    team_elements = teams_container.select('.c-events__team')
                    if len(team_elements) >= 2:
                        match_data['team1'] = team_elements[0].text.strip()
                        match_data['team2'] = team_elements[1].text.strip()
                        print(f"Match {match_index+1}: {match_data['team1']} vs {match_data['team2']}")
                
                # Get match status and time
                time_element = match.select_one('.c-events__time')
                if time_element:
                    match_data['status'] = time_element.get_text(strip=True, separator=' ')
                
                # Get score - handling different score display formats
                score_cells = match.select('.c-events-scoreboard__cell--all')
                if score_cells:
                    scores = []
                    for score in score_cells:
                        if score.text.strip():
                            scores.append(score.text.strip())
                    
                    if scores:
                        match_data['scores'] = scores
                        match_data['score'] = ' - '.join(scores)
                
                # Create a unique ID for the match
                if 'team1' in match_data and 'team2' in match_data:
                    match_data['match_id'] = f"{sport_name}_{league_name}_{match_data['team1']}_{match_data['team2']}"
                else:
                    match_data['match_id'] = f"{sport_name}_{league_name}_{match_index}"
                
                # Extract odds
                self.extract_odds(match, bet_types, match_data)
                
                # Get the match URL
                match_url_element = match.select_one('a.c-events__name')
                if match_url_element and 'href' in match_url_element.attrs:
                    match_data['match_url'] = match_url_element['href']
                
                # Capture any other important data
                icons = match.select('.c-events__ico')
                if icons:
                    match_data['has_video'] = any('c-events__ico_video' in icon.get('class', []) for icon in icons)
                    match_data['has_statistics'] = any('c-events__ico--statistics' in icon.get('class', []) for icon in icons)
                
                live_events.append(match_data)
                
        logging.info(f"Successfully parsed {len(live_events)} live events")
        print(f"Successfully parsed {len(live_events)} live events")
        return live_events
    
    def extract_odds(self, match, bet_types, match_data):
        """Enhanced odds extraction specifically designed for 1xbet structure"""
        # Find the c-bets container which holds all odds
        bets_container = match.select_one('.c-bets')
        odds_found = False
        
        if not bets_container:
            print(f"No .c-bets container found for {match_data.get('team1', '')} vs {match_data.get('team2', '')}")
            # Try to find parent and then look for .c-bets
            parent = match.parent
            if parent:
                bets_container = parent.select_one('.c-bets')
            
            if not bets_container:
                # Look for .c-bets in siblings
                next_sibling = match.next_sibling
                if next_sibling:
                    bets_container = next_sibling if hasattr(next_sibling, 'get') and 'c-bets' in next_sibling.get('class', []) else None
                
                if not bets_container:
                    # Last resort: look in any adjacent element
                    adjacent_elements = match.find_next_siblings()
                    for elem in adjacent_elements:
                        if 'c-bets' in elem.get('class', []):
                            bets_container = elem
                            break
        
        if bets_container:
            # Get all bet cells (both regular and 'non' odds)
            all_bet_cells = bets_container.select('.c-bets__bet')
            print(f"Found {len(all_bet_cells)} total bet cells for {match_data.get('team1', '')} vs {match_data.get('team2', '')}")
            
            # Process each bet cell
            for i, cell in enumerate(all_bet_cells):
                # Get the title (bet type) if available
                bet_title = cell.get('title')
                
                # Check if this is a valid odd (not a 'non' cell)
                is_valid = 'non' not in cell.get('class', [])
                
                # Set the bet_type from either title or index in bet_types list
                if bet_title:
                    bet_type = bet_title.strip()
                elif i < len(bet_types):
                    bet_type = bet_types[i]
                else:
                    bet_type = f"odd_{i+1}"
                
                # Process if this is a valid odd
                if is_valid:
                    inner_span = cell.select_one('.c-bets__inner')
                    if inner_span:
                        odds_value = inner_span.text.strip()
                        if odds_value and odds_value != '–' and odds_value != '-':  # Skip placeholder values
                            # Clean up the bet_type for use as a key
                            bet_key = bet_type.replace(' ', '_').lower()
                            
                            # Store both the raw title and the value
                            match_data[f'odd_{bet_key}'] = odds_value
                            match_data[f'odd_title_{bet_key}'] = bet_title if bet_title else f"Market {i+1}"
                            
                            print(f"✓ Extracted odds for {bet_type}: {odds_value}")
                            odds_found = True
            
            # Additionally, also extract odds by index position for consistency
            valid_bet_cells = [cell for cell in all_bet_cells if 'non' not in cell.get('class', [])]
            for i, cell in enumerate(valid_bet_cells):
                inner_span = cell.select_one('.c-bets__inner')
                if inner_span:
                    odds_value = inner_span.text.strip()
                    if odds_value and odds_value != '–' and odds_value != '-':
                        match_data[f'odd_position_{i+1}'] = odds_value
        
        # If no odds were found, report it but don't store debug HTML (to save memory)
        if not odds_found:
            print(f"⚠ No odds found for {match_data.get('team1', '')} vs {match_data.get('team2', '')}")
    
    def parse_upcoming_events(self, html_content):
        """Parse the upcoming (non-live) events section of the page"""
        soup = BeautifulSoup(html_content, 'html.parser')
        upcoming_events = []
        
        # Find the Sportsbook section (blueBack container)
        upcoming_container = soup.select_one('div[id="line_bets_on_main"].c-events.blueBack')
        if not upcoming_container:
            logging.warning("Upcoming events container not found")
            print("Upcoming events container not found")
            return upcoming_events
            
        # Find all upcoming events containers
        upcoming_sections = upcoming_container.select('.dashboard-champ-content')
        logging.info(f"Found {len(upcoming_sections)} upcoming sections")
        print(f"Found {len(upcoming_sections)} upcoming sections")
        
        for section_index, section in enumerate(upcoming_sections):
            # Get league info
            league_header = section.select_one('.c-events__item_head')
            if not league_header:
                print(f"No header found for section {section_index}")
                continue
                
            # Get sport type
            sport_icon = league_header.select_one('.icon use')
            sport_type = sport_icon['xlink:href'].split('#')[-1].replace('sports_', '') if sport_icon else "Unknown"
            sport_name = self.get_sport_name(sport_type)
            
            # Get country
            country_element = league_header.select_one('.flag-icon use')
            country = country_element['xlink:href'].split('#')[-1] if country_element else "International"
            
            # Get league name
            league_name_element = league_header.select_one('.c-events__liga')
            league_name = league_name_element.text.strip() if league_name_element else "Unknown League"
            league_url = league_name_element['href'] if league_name_element and 'href' in league_name_element.attrs else ""
            
            print(f"Processing league: {league_name} ({sport_name})")
            
            # Get the available bet types for this league
            bet_types = []
            bet_title_elements = league_header.select('.c-bets__title')
            for title_elem in bet_title_elements:
                bet_types.append(title_elem.text.strip())
            
            # Track current date for all matches in this section
            current_date = None
            
            # Get all matches in this league
            match_items = section.select('.c-events__item_col')
            
            for item_index, item in enumerate(match_items):
                # Check if this is a date header
                date_element = item.select_one('.c-events__date')
                if date_element:
                    current_date = date_element.text.strip()
                    print(f"Found date: {current_date}")
                    continue
                
                # Get match element
                match = item.select_one('.c-events__item_game')
                if not match:
                    continue
                
                match_data = {
                    'sport': sport_name,
                    'country': country,
                    'league': league_name,
                    'league_url': league_url,
                    'match_date': current_date,
                    'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
                
                # Get team names
                teams_container = match.select_one('.c-events__teams')
                if teams_container:
                    team_elements = teams_container.select('.c-events__team')
                    if len(team_elements) >= 2:
                        match_data['team1'] = team_elements[0].text.strip()
                        match_data['team2'] = team_elements[1].text.strip()
                        print(f"Match {item_index}: {match_data['team1']} vs {match_data['team2']}")
                
                # Get match time
                time_element = match.select_one('.c-events-time__val')
                if time_element:
                    match_data['start_time'] = time_element.text.strip()
                
                # Create a unique ID for the match
                if 'team1' in match_data and 'team2' in match_data:
                    match_data['match_id'] = f"{sport_name}_{league_name}_{match_data['team1']}_{match_data['team2']}"
                else:
                    match_data['match_id'] = f"{sport_name}_{league_name}_{item_index}"
                
                # Use the improved odds extraction method
                self.extract_odds(match, bet_types, match_data)
                
                # Get the match URL
                match_url_element = match.select_one('a.c-events__name')
                if match_url_element and 'href' in match_url_element.attrs:
                    match_data['match_url'] = match_url_element['href']
                
                # Capture starting time info
                starts_in_element = match.select_one('div[title^="Starts in"]')
                if starts_in_element:
                    starts_in_text = starts_in_element.get('title', '')
                    match_data['starts_in'] = starts_in_text.replace('Starts in ', '')
                
                upcoming_events.append(match_data)
                
        logging.info(f"Successfully parsed {len(upcoming_events)} upcoming events")
        print(f"Successfully parsed {len(upcoming_events)} upcoming events")
        return upcoming_events
    
    def get_sport_name(self, sport_id):
        """Convert sport ID to readable name"""
        sport_mapping = {
            '1': 'Football',
            '2': 'Ice Hockey',
            '3': 'Basketball',
            '4': 'Tennis',
            '10': 'Table Tennis',
            '66': 'Cricket',
            '85': 'FIFA',
            '95': 'Volleyball',
            '17': 'Hockey',
            '29': 'Baseball',
            '107': 'Darts',
            '128': 'Handball',
        }
        return sport_mapping.get(sport_id, f"Sport {sport_id}")
    
    def get_all_leagues(self, html_content=None):
        """Get a list of all available leagues on the homepage"""
        if not html_content:
            html_content = self.get_page_content()
            if not html_content:
                return []
        
        soup = BeautifulSoup(html_content, 'html.parser')
        leagues = []
        
        # Find all league headers across both live and upcoming sections
        league_headers = soup.select('.c-events__item_head')
        logging.info(f"Found {len(league_headers)} league headers")
        print(f"Found {len(league_headers)} league headers")
        
        for i, header in enumerate(league_headers):
            # Skip duplicate leagues
            league_element = header.select_one('.c-events__liga')
            if not league_element:
                continue
            
            # Get information about this league
            league_name = league_element.text.strip()
            league_url = league_element['href'] if 'href' in league_element.attrs else ""
            
            # Get sport type
            sport_icon = header.select_one('.icon use')
            sport_type = sport_icon['xlink:href'].split('#')[-1].replace('sports_', '') if sport_icon else "Unknown"
            sport_name = self.get_sport_name(sport_type)
            
            # Get country
            country_element = header.select_one('.flag-icon use')
            country = country_element['xlink:href'].split('#')[-1] if country_element else "International"
            
            # Create league data object
            league_data = {
                'name': league_name,
                'url': league_url,
                'sport': sport_name,
                'country': country,
                'league_id': f"{sport_name}_{league_name}"
            }
            
            # Check if this is a top event
            is_top_section = header.find_parent('div', class_='top-champs-banner')
            if is_top_section:
                league_data['is_top_event'] = True
            
            # Avoid duplicates
            if not any(l['league_id'] == league_data['league_id'] for l in leagues):
                leagues.append(league_data)
                print(f"League {i+1}: {league_data['name']} ({league_data['sport']})")
        
        return leagues
    
    def update_match_odds(self, existing_match, new_match):
        """Update odds and any changed data in an existing match with new data"""
        # Track if odds have changed
        odds_changed = False
        
        # Update timestamp
        existing_match['timestamp'] = new_match['timestamp']
        
        # Update score if available
        if 'score' in new_match:
            if 'score' not in existing_match or existing_match['score'] != new_match['score']:
                print(f"Score updated for {existing_match.get('team1', '')} vs {existing_match.get('team2', '')}: {existing_match.get('score', 'No score')} → {new_match['score']}")
                existing_match['score'] = new_match['score']
                odds_changed = True
        
        # Update scores array if available
        if 'scores' in new_match:
            if 'scores' not in existing_match or existing_match['scores'] != new_match['scores']:
                existing_match['scores'] = new_match['scores']
        
        # Update match status if available
        if 'status' in new_match and ('status' not in existing_match or existing_match['status'] != new_match['status']):
            existing_match['status'] = new_match['status']
            odds_changed = True
        
        # Update all other fields
        for key, value in new_match.items():
            # Skip already handled fields and the match_id
            if key in ['timestamp', 'score', 'scores', 'status', 'match_id']:
                continue
                
            # Check if it's an odds field that has changed
            if key.startswith('odd_'):
                if key not in existing_match or existing_match[key] != value:
                    print(f"Odds updated for {existing_match.get('team1', '')} vs {existing_match.get('team2', '')}: {key} changed from {existing_match.get(key, 'N/A')} → {value}")
                    existing_match[key] = value
                    odds_changed = True
            # Update any other fields that might have changed
            elif key not in existing_match or existing_match[key] != value:
                existing_match[key] = value
        
        return odds_changed
    
    def save_to_csv(self, data, filename, append=False):
        """Save the scraped data to a CSV file"""
        if not data:
            print(f"No data to save to {filename}")
            return
        
        # Ensure the filename includes the data directory
        filepath = os.path.join(self.data_dir, filename)
        
        mode = 'a' if append else 'w'
        header = not append or not os.path.exists(filepath)
        
        try:
            df = pd.DataFrame(data)
            df.to_csv(filepath, mode=mode, index=False, header=header)
            
            if append:
                print(f"Data appended to {filepath}")
            else:
                print(f"Data saved to {filepath} with {len(data)} records")
        except Exception as e:
            logging.error(f"Error saving CSV {filepath}: {e}")
            print(f"Error saving CSV {filepath}: {e}")
    
    def save_to_json(self, data, filename):
        """Save the scraped data to a JSON file"""
        if not data:
            print(f"No data to save to {filename}")
            return
        
        filepath = os.path.join(self.data_dir, filename)
        
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            print(f"Data saved to {filepath} with {len(data)} records")
        except Exception as e:
            logging.error(f"Error saving JSON {filepath}: {e}")
            print(f"Error saving JSON {filepath}: {e}")
    
    def send_ping(self):
        """Send a ping to a health check service to confirm the service is running"""
        ping_url = os.environ.get("PING_URL")
        if ping_url:
            try:
                requests.get(ping_url, timeout=10)
                logging.info(f"Health ping sent to {ping_url}")
                print(f"Health ping sent to {ping_url}")
            except Exception as e:
                logging.error(f"Failed to send health ping: {e}")
                print(f"Failed to send health ping: {e}")
    
    def restart_driver_if_needed(self):
        """Check if we should restart the driver (every few hours to prevent memory issues)"""
        if not hasattr(self, 'driver_start_time'):
            self.driver_start_time = datetime.now()
            return
            
        # Restart driver every 4 hours to prevent memory leaks
        hours_since_start = (datetime.now() - self.driver_start_time).total_seconds() / 3600
        if hours_since_start > 4:
            logging.info("Restarting driver to prevent memory issues...")
            print("Restarting driver to prevent memory issues...")
            try:
                if self.driver:
                    self.driver.quit()
            except:
                logging.error("Error closing driver during scheduled restart")
                print("Error closing driver during scheduled restart")
            
            self.driver = None
            self.driver_start_time = datetime.now()  # Reset timer
    
    def cleanup_old_data(self):
        """Remove old data to prevent disk space issues"""
        # Keep only data from the last 48 hours
        cutoff_time = datetime.now() - timedelta(hours=48)
        cutoff_str = cutoff_time.strftime("%Y-%m-%d %H:%M:%S")
        
        # Cleanup live events
        if self.live_events:
            before_count = len(self.live_events)
            self.live_events = [m for m in self.live_events if 'timestamp' not in m or m['timestamp'] > cutoff_str]
            after_count = len(self.live_events)
            if before_count > after_count:
                logging.info(f"Cleaned up {before_count - after_count} old live events")
                print(f"Cleaned up {before_count - after_count} old live events")
        
        # Cleanup upcoming events
        if self.upcoming_events:
            before_count = len(self.upcoming_events)
            self.upcoming_events = [m for m in self.upcoming_events if 'timestamp' not in m or m['timestamp'] > cutoff_str]
            after_count = len(self.upcoming_events)
            if before_count > after_count:
                logging.info(f"Cleaned up {before_count - after_count} old upcoming events")
                print(f"Cleaned up {before_count - after_count} old upcoming events")
        
        # Cleanup old files - keep only the latest 5 of each type
        try:
            for pattern in ["*_events_*.json", "*_events_*.csv"]:
                files = glob.glob(os.path.join(self.data_dir, pattern))
                files.sort(key=os.path.getmtime)
                if len(files) > 5:
                    for old_file in files[:-5]:
                        try:
                            os.remove(old_file)
                            logging.info(f"Removed old file: {old_file}")
                            print(f"Removed old file: {old_file}")
                        except Exception as e:
                            logging.error(f"Error removing file {old_file}: {e}")
                            print(f"Error removing file {old_file}: {e}")
        except Exception as e:
            logging.error(f"Error during file cleanup: {e}")
            print(f"Error during file cleanup: {e}")
    
    def run_continuous_updates(self):
        """Run continuous updates of odds with resilience for Render's environment"""
        logging.info(f"Starting continuous odds updates every {self.update_interval} seconds")
        print(f"Starting continuous odds updates every {self.update_interval} seconds")
        logging.info(f"Data will be saved to directory: {self.data_dir}")
        print(f"Data will be saved to directory: {self.data_dir}")
        
        update_count = 0
        consecutive_errors = 0
        max_consecutive_errors = 5
        
        try:
            # Initialize with first fetch
            html_content = self.get_page_content()
            if html_content:
                # Initial parsing
                self.live_events = self.parse_live_events(html_content)
                self.upcoming_events = self.parse_upcoming_events(html_content)
                self.leagues = self.get_all_leagues(html_content)
                
                # Save initial data
                self.save_to_csv(self.live_events, "1xbet_live_events.csv")
                self.save_to_csv(self.upcoming_events, "1xbet_upcoming_events.csv")
                self.save_to_csv(self.leagues, "1xbet_leagues.csv")
                
                self.save_to_json(self.live_events, "1xbet_live_events.json")
                self.save_to_json(self.upcoming_events, "1xbet_upcoming_events.json")
                self.save_to_json(self.leagues, "1xbet_leagues.json")
                
                # Create a log file for odds changes
                with open(os.path.join(self.data_dir, "odds_changes_log.txt"), "w", encoding="utf-8") as f:
                    f.write(f"Odds changes log - Started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                    f.write("-" * 80 + "\n")
                
                consecutive_errors = 0  # Reset error counter on success
            else:
                logging.error("Failed to retrieve initial data. Will try again on next cycle.")
                print("Failed to retrieve initial data. Will try again on next cycle.")
                consecutive_errors += 1
            
            # Main update loop
            while self.running:
                update_count += 1
                logging.info(f"=== Update #{update_count} at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===")
                print(f"\n=== Update #{update_count} at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===")
                
                # Check if we need to restart the driver
                self.restart_driver_if_needed()
                
                # Send a health check ping every 10 updates
                if update_count % 10 == 0:
                    self.send_ping()
                
                # Cleanup old data every 50 updates
                if update_count % 50 == 0:
                    logging.info("Performing data cleanup...")
                    print("Performing data cleanup...")
                    self.cleanup_old_data()
                
                # Force garbage collection every 20 updates
                if update_count % 20 == 0:
                    logging.info("Running garbage collection...")
                    print("Running garbage collection...")
                    gc.collect()
                
                # Wait for the next update
                time.sleep(self.update_interval)
                
                try:
                    # Keep track of previous data to check for changes
                    old_live_events = self.live_events.copy() if self.live_events else []
                    old_upcoming_events = self.upcoming_events.copy() if self.upcoming_events else []
                    
                    # Refresh page content
                    html_content = self.get_page_content()
                    if not html_content:
                        logging.error("Failed to retrieve the page. Skipping this update.")
                        print("Failed to retrieve the page. Skipping this update.")
                        consecutive_errors += 1
                        if consecutive_errors >= max_consecutive_errors:
                            logging.error(f"Too many consecutive errors ({consecutive_errors}). Restarting driver...")
                            print(f"Too many consecutive errors ({consecutive_errors}). Restarting driver...")
                            if self.driver:
                                try:
                                    self.driver.quit()
                                except:
                                    pass
                            self.driver = None
                        continue
                    
                    # Reset error counter on success
                    consecutive_errors = 0
                    
                    # Parse updated data
                    new_live_events = self.parse_live_events(html_content)
                    new_upcoming_events = self.parse_upcoming_events(html_content)
                    
                    # Create maps for quick lookup by match_id
                    live_map = {match['match_id']: match for match in self.live_events if 'match_id' in match}
                    upcoming_map = {match['match_id']: match for match in self.upcoming_events if 'match_id' in match}
                    
                    # Track which matches have changed
                    changed_live_matches = []
                    changed_upcoming_matches = []
                    new_live_matches = []
                    new_upcoming_matches = []
                    
                    # Update live events
                    for new_match in new_live_events:
                        if 'match_id' not in new_match:
                            continue
                            
                        match_id = new_match['match_id']
                        if match_id in live_map:
                            if self.update_match_odds(live_map[match_id], new_match):
                                changed_live_matches.append(live_map[match_id])
                        else:
                            new_live_matches.append(new_match)
                            self.live_events.append(new_match)
                    
                    # Update upcoming events
                    for new_match in new_upcoming_events:
                        if 'match_id' not in new_match:
                            continue
                            
                        match_id = new_match['match_id']
                        if match_id in upcoming_map:
                            if self.update_match_odds(upcoming_map[match_id], new_match):
                                changed_upcoming_matches.append(upcoming_map[match_id])
                        else:
                            new_upcoming_matches.append(new_match)
                            self.upcoming_events.append(new_match)
                    
                    # Log changes
                    logging.info(f"Live events: {len(self.live_events)} total, {len(new_live_matches)} new, {len(changed_live_matches)} updated")
                    print(f"Live events: {len(self.live_events)} total, {len(new_live_matches)} new, {len(changed_live_matches)} updated")
                    logging.info(f"Upcoming events: {len(self.upcoming_events)} total, {len(new_upcoming_matches)} new, {len(changed_upcoming_matches)} updated")
                    print(f"Upcoming events: {len(self.upcoming_events)} total, {len(new_upcoming_matches)} new, {len(changed_upcoming_matches)} updated")
                    
                    # Save updated data - only save if there are changes to reduce disk I/O
                    if changed_live_matches or new_live_matches:
                        timestamp = datetime.now().strftime("%Y%m%d-%H%M")
                        self.save_to_json(self.live_events, f"1xbet_live_events.json")
                        # Save a timestamped version every 10 updates
                        if update_count % 10 == 0:
                            self.save_to_json(self.live_events, f"1xbet_live_events_{timestamp}.json")
                        
                        # Append only new matches to CSV
                        if new_live_matches:
                            self.save_to_csv(new_live_matches, "1xbet_live_events.csv", append=True)
                    
                    if changed_upcoming_matches or new_upcoming_matches:
                        timestamp = datetime.now().strftime("%Y%m%d-%H%M")
                        self.save_to_json(self.upcoming_events, f"1xbet_upcoming_events.json")
                        # Save a timestamped version every 10 updates
                        if update_count % 10 == 0:
                            self.save_to_json(self.upcoming_events, f"1xbet_upcoming_events_{timestamp}.json")
                        
                        # Append only new matches to CSV
                        if new_upcoming_matches:
                            self.save_to_csv(new_upcoming_matches, "1xbet_upcoming_events.csv", append=True)
                    
                    # Log odds changes to file
                    if changed_live_matches or changed_upcoming_matches:
                        with open(os.path.join(self.data_dir, "odds_changes_log.txt"), "a", encoding="utf-8") as f:
                            f.write(f"\nUpdate at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                            
                            for match in changed_live_matches:
                                old_match = next((m for m in old_live_events if 'match_id' in m and m['match_id'] == match['match_id']), {})
                                for key in match:
                                    if key.startswith('odd_') and key in old_match and old_match[key] != match[key]:
                                        f.write(f"LIVE: {match.get('team1', '')} vs {match.get('team2', '')}: {key} {old_match[key]} → {match[key]}\n")
                            
                            for match in changed_upcoming_matches:
                                old_match = next((m for m in old_upcoming_events if 'match_id' in m and m['match_id'] == match['match_id']), {})
                                for key in match:
                                    if key.startswith('odd_') and key in old_match and old_match[key] != match[key]:
                                        f.write(f"UPCOMING: {match.get('team1', '')} vs {match.get('team2', '')}: {key} {old_match[key]} → {match[key]}\n")
                    
                    # Every 30 updates, update the leagues - reduced frequency to save resources
                    if update_count % 30 == 0:
                        logging.info("Updating leagues list...")
                        print("Updating leagues list...")
                        new_leagues = self.get_all_leagues(html_content)
                        if new_leagues:
                            self.leagues = new_leagues
                            self.save_to_json(self.leagues, "1xbet_leagues.json")
                            self.save_to_csv(self.leagues, "1xbet_leagues.csv")
                    
                except Exception as e:
                    logging.error(f"Error during update cycle: {e}")
                    logging.error(traceback.format_exc())
                    print(f"Error during update cycle: {e}")
                    print(traceback.format_exc())
                    consecutive_errors += 1
                    
                    # If we have too many errors in a row, try restarting the driver
                    if consecutive_errors >= max_consecutive_errors:
                        logging.error(f"Too many consecutive errors ({consecutive_errors}). Restarting driver...")
                        print(f"Too many consecutive errors ({consecutive_errors}). Restarting driver...")
                        if self.driver:
                            try:
                                self.driver.quit()
                            except:
                                pass
                        self.driver = None
                        consecutive_errors = 0  # Reset counter after restart attempt
                
        except Exception as e:
            logging.critical(f"Critical error in run_continuous_updates: {e}")
            logging.critical(traceback.format_exc())
            print(f"Critical error in run_continuous_updates: {e}")
            print(traceback.format_exc())
        finally:
            logging.info("Saving final data...")
            print("\nSaving final data...")
            # Save the final data
            timestamp = datetime.now().strftime("%Y%m%d-%H%M")
            self.save_to_csv(self.live_events, f"1xbet_live_events_final_{timestamp}.csv")
            self.save_to_csv(self.upcoming_events, f"1xbet_upcoming_events_final_{timestamp}.csv")
            self.save_to_json(self.live_events, f"1xbet_live_events_final_{timestamp}.json")
            self.save_to_json(self.upcoming_events, f"1xbet_upcoming_events_final_{timestamp}.json")
            logging.info("Final data saved. Shutting down...")
            print("Final data saved. Shutting down...")
    
    def run(self, mode="continuous"):
        """Run the scraping process"""
        logging.info("Starting 1xbet scraper...")
        print("Starting 1xbet scraper...")
        logging.info(f"Running in {mode} mode")
        print(f"Running in {mode} mode")
        
        if mode == "continuous":
            self.run_continuous_updates()
        else:
            # Single run mode
            html_content = self.get_page_content()
            if not html_content:
                logging.error("Failed to retrieve the main page. Exiting.")
                print("Failed to retrieve the main page. Exiting.")
                return
            
            # Parse live events
            logging.info("Parsing live events...")
            print("Parsing live events...")
            live_events = self.parse_live_events(html_content)
            
            # Parse upcoming events
            logging.info("Parsing upcoming events...")
            print("Parsing upcoming events...")
            upcoming_events = self.parse_upcoming_events(html_content)
            
            # Get all leagues
            logging.info("Getting all leagues...")
            print("Getting all leagues...")
            leagues = self.get_all_leagues(html_content)
            
            # Save the scraped data
            self.save_to_csv(live_events, "1xbet_live_events.csv")
            self.save_to_csv(upcoming_events, "1xbet_upcoming_events.csv")
            self.save_to_csv(leagues, "1xbet_leagues.csv")
            
            self.save_to_json(live_events, "1xbet_live_events.json")
            self.save_to_json(upcoming_events, "1xbet_upcoming_events.json")
            self.save_to_json(leagues, "1xbet_leagues.json")
            
            logging.info("Scraping completed successfully!")
            print("Scraping completed successfully!")

if __name__ == "__main__":
    print(f"Script started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Python version: {sys.version}")
    logging.info(f"Script started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logging.info(f"Python version: {sys.version}")
    
    # Get port from environment or use default
    port = int(os.environ.get('PORT', 10000))
    
    # Start web server in a separate thread
    server_thread = threading.Thread(target=run_server, args=(port,), daemon=True)
    server_thread.start()
    
    # Create and run the scraper
    retry_count = 0
    max_retries = 5
    
    while retry_count < max_retries:
        try:
            scraper = XbetScraper()
            global_scraper = scraper  # Set global reference for web server
            scraper.run(mode="continuous")  # Run in continuous update mode
            break
        except Exception as e:
            retry_count += 1
            error_msg = f"Error running scraper (attempt {retry_count}/{max_retries}): {e}"
            logging.error(error_msg)
            logging.error(traceback.format_exc())
            print(error_msg)
            print(traceback.format_exc())
            
            # Wait before retry
            time.sleep(60)
            
            # If we've reached max retries, log and exit
            if retry_count >= max_retries:
                final_error = "Max retries reached. Exiting."
                logging.error(final_error)
                print(final_error)
