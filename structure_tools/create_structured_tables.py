#!/usr/bin/env python3
"""
Structured Data Tables Generator
===============================

This script organizes decrypted survey data into structured, analysis-ready tables.
It takes the output from the automated decryption pipeline and creates 4 normalized tables:

1. **participants** - Master participant table with demographics
2. **biweekly_responses** - Time-series wellbeing survey data  
3. **consent_records** - Data sharing consent tracking
4. **location_tracks** - GPS location data with temporal organization

Features:
- Standardized column naming and data types
- Proper participant ID linking across all tables
- Temporal organization for time-series analysis
- Data validation and quality checks
- Export to multiple formats (CSV, JSON, SQLite)

Usage:
    python3 create_structured_tables.py [options]
    
Examples:
    # Process all decrypted data
    python3 create_structured_tables.py --input ./decrypted_data
    
    # Create SQLite database
    python3 create_structured_tables.py --input ./decrypted_data --database research_data.db
    
    # Generate analysis summary
    python3 create_structured_tables.py --input ./decrypted_data --summary
"""

import argparse
import json
import csv
import sqlite3
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
import pandas as pd
from dataclasses import dataclass, asdict
import re

@dataclass
class Participant:
    """Participant demographics and metadata"""
    participant_id: str
    age: Optional[int] = None
    gender: Optional[str] = None
    ethnicity: Optional[str] = None
    education: Optional[str] = None
    employment: Optional[str] = None
    income: Optional[str] = None
    household_size: Optional[int] = None
    location_area: Optional[str] = None
    registration_date: Optional[str] = None
    consent_status: Optional[str] = None
    
@dataclass
class BiweeklyResponse:
    """Biweekly wellbeing survey response"""
    response_id: str
    participant_id: str
    submission_date: str
    survey_week: Optional[int] = None
    
    # Wellbeing measures
    happiness_score: Optional[float] = None
    stress_level: Optional[float] = None
    life_satisfaction: Optional[float] = None
    health_rating: Optional[float] = None
    
    # Activity and mobility
    activity_level: Optional[str] = None
    places_visited: Optional[int] = None
    travel_distance: Optional[float] = None
    transport_modes: Optional[str] = None
    
    # Social and environmental
    social_interactions: Optional[int] = None
    green_space_time: Optional[float] = None
    indoor_time: Optional[float] = None
    
    # Data completeness
    has_location_data: bool = False
    location_points_count: Optional[int] = None
    data_quality_score: Optional[float] = None

@dataclass
class ConsentRecord:
    """Data sharing consent record"""
    consent_id: str
    participant_id: str
    consent_date: str
    consent_type: str
    
    # Consent details
    data_sharing_approved: bool = False
    location_sharing_approved: bool = False
    research_contact_approved: bool = False
    data_retention_period: Optional[str] = None
    
    # Legal compliance
    informed_consent_version: Optional[str] = None
    withdrawal_date: Optional[str] = None
    withdrawal_reason: Optional[str] = None

@dataclass
class LocationTrack:
    """GPS location data point"""
    track_id: str
    participant_id: str
    response_id: str
    timestamp: str
    
    # Coordinates
    latitude: float
    longitude: float
    accuracy: Optional[float] = None
    altitude: Optional[float] = None
    
    # Movement data
    speed: Optional[float] = None
    heading: Optional[float] = None
    
    # Temporal organization
    date: Optional[str] = None
    time: Optional[str] = None
    day_of_week: Optional[str] = None
    hour_of_day: Optional[int] = None
    
    # Analysis fields
    stay_point: bool = False
    activity_type: Optional[str] = None
    location_context: Optional[str] = None

class StructuredDataTablesGenerator:
    """Generate structured, analysis-ready tables from decrypted survey data"""
    
    def __init__(self, input_dir: str, output_dir: str = './data/structured'):
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.participants = []
        self.biweekly_responses = []
        self.consent_records = []
        self.location_tracks = []
        
        # Data quality tracking
        self.processing_stats = {
            'files_processed': 0,
            'participants_found': 0,
            'responses_processed': 0,
            'location_points': 0,
            'data_quality_issues': []
        }
        
        # Column mappings for different survey formats
        self.initial_survey_columns = {
            'participant_id': ['ResponseId', 'participantId', 'participant_id', 'Q1'],
            'age': ['Q2', 'age', 'Age'],
            'gender': ['Q3', 'gender', 'Gender'],
            'ethnicity': ['Q4', 'ethnicity', 'Ethnicity'],
            'education': ['Q5', 'education', 'Education'],
            'employment': ['Q6', 'employment', 'Employment'],
            'income': ['Q7', 'income', 'Income'],
            'household_size': ['Q8', 'household_size', 'HouseholdSize'],
            'location_area': ['Q9', 'location_area', 'LocationArea']
        }
        
        self.biweekly_survey_columns = {
            'response_id': ['ResponseId', 'response_id'],
            'participant_id': ['Q1', 'participantId', 'participant_id'],
            'happiness_score': ['Q2', 'happiness', 'happiness_score'],
            'stress_level': ['Q3', 'stress', 'stress_level'],
            'life_satisfaction': ['Q4', 'satisfaction', 'life_satisfaction'],
            'health_rating': ['Q5', 'health', 'health_rating'],
            'activity_level': ['Q6', 'activity', 'activity_level'],
            'places_visited': ['Q7', 'places', 'places_visited'],
            'social_interactions': ['Q8', 'social', 'social_interactions'],
            'green_space_time': ['Q9', 'green_space', 'green_space_time'],
            'transport_modes': ['Q10', 'transport', 'transport_modes']
        }
    
    def process_all_data(self) -> bool:
        """Process all decrypted CSV files and generate structured tables"""
        
        if not os.path.exists(self.input_dir):
            print(f"❌ Input directory not found: {self.input_dir}")
            return False
        
        print(f"🔄 Processing decrypted data from: {self.input_dir}")
        
        # Create output directory
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Process different types of survey data
        success = True
        success &= self._process_initial_surveys()
        success &= self._process_biweekly_surveys()
        success &= self._process_consent_data()
        success &= self._process_location_data()
        
        if success:
            self._generate_output_files()
            self._generate_summary_report()
            print(f"✅ Structured data tables created successfully in: {self.output_dir}")
        
        return success
    
    def _process_initial_surveys(self) -> bool:
        """Process initial demographics survey data"""
        
        files = self._find_files(['initial_decrypted_responses.csv', 'initial_survey_responses.csv'])
        if not files:
            print("⚠️ No initial survey files found")
            return True
        
        for file_path in files:
            print(f"📊 Processing initial survey data: {os.path.basename(file_path)}")
            
            try:
                df = pd.read_csv(file_path)
                print(f"📋 Found {len(df)} initial survey responses")
                
                for _, row in df.iterrows():
                    participant = self._extract_participant_data(row)
                    if participant:
                        self.participants.append(participant)
                        
                self.processing_stats['files_processed'] += 1
                self.processing_stats['participants_found'] = len(self.participants)
                
            except Exception as e:
                print(f"❌ Error processing {file_path}: {e}")
                self.processing_stats['data_quality_issues'].append(f"Initial survey processing error: {e}")
                return False
        
        return True
    
    def _process_biweekly_surveys(self) -> bool:
        """Process biweekly wellbeing survey data"""
        
        files = self._find_files(['biweekly_decrypted_responses.csv', 'biweekly_survey_responses.csv'])
        if not files:
            print("⚠️ No biweekly survey files found")
            return True
        
        for file_path in files:
            print(f"📊 Processing biweekly survey data: {os.path.basename(file_path)}")
            
            try:
                df = pd.read_csv(file_path)
                print(f"📋 Found {len(df)} biweekly survey responses")
                
                for _, row in df.iterrows():
                    response = self._extract_biweekly_data(row)
                    if response:
                        self.biweekly_responses.append(response)
                        
                self.processing_stats['files_processed'] += 1
                self.processing_stats['responses_processed'] = len(self.biweekly_responses)
                
            except Exception as e:
                print(f"❌ Error processing {file_path}: {e}")
                self.processing_stats['data_quality_issues'].append(f"Biweekly survey processing error: {e}")
                return False
        
        return True
    
    def _process_consent_data(self) -> bool:
        """Process consent form data"""
        
        files = self._find_files(['consent_decrypted_responses.csv', 'consent_form_responses.csv'])
        if not files:
            print("⚠️ No consent form files found")
            return True
        
        for file_path in files:
            print(f"📊 Processing consent data: {os.path.basename(file_path)}")
            
            try:
                df = pd.read_csv(file_path)
                print(f"📋 Found {len(df)} consent records")
                
                for _, row in df.iterrows():
                    consent = self._extract_consent_data(row)
                    if consent:
                        self.consent_records.append(consent)
                        
                self.processing_stats['files_processed'] += 1
                
            except Exception as e:
                print(f"❌ Error processing {file_path}: {e}")
                self.processing_stats['data_quality_issues'].append(f"Consent data processing error: {e}")
                return False
        
        return True
    
    def _process_location_data(self) -> bool:
        """Process location tracking data"""
        
        files = self._find_files(['biweekly_decrypted_locations.csv', 'location_data.csv'])
        if not files:
            print("⚠️ No location data files found")
            return True
        
        for file_path in files:
            print(f"📍 Processing location data: {os.path.basename(file_path)}")
            
            try:
                df = pd.read_csv(file_path)
                print(f"📋 Found {len(df)} location points")
                
                for _, row in df.iterrows():
                    track_point = self._extract_location_data(row)
                    if track_point:
                        self.location_tracks.append(track_point)
                        
                self.processing_stats['files_processed'] += 1
                self.processing_stats['location_points'] = len(self.location_tracks)
                
            except Exception as e:
                print(f"❌ Error processing {file_path}: {e}")
                self.processing_stats['data_quality_issues'].append(f"Location data processing error: {e}")
                return False
        
        return True
    
    def _extract_participant_data(self, row: pd.Series) -> Optional[Participant]:
        """Extract participant demographics from survey row"""
        
        try:
            # Find participant ID
            participant_id = self._find_value(row, self.initial_survey_columns['participant_id'])
            if not participant_id:
                return None
            
            # Extract demographics
            age = self._safe_int(self._find_value(row, self.initial_survey_columns['age']))
            gender = self._find_value(row, self.initial_survey_columns['gender'])
            ethnicity = self._find_value(row, self.initial_survey_columns['ethnicity'])
            education = self._find_value(row, self.initial_survey_columns['education'])
            employment = self._find_value(row, self.initial_survey_columns['employment'])
            income = self._find_value(row, self.initial_survey_columns['income'])
            household_size = self._safe_int(self._find_value(row, self.initial_survey_columns['household_size']))
            location_area = self._find_value(row, self.initial_survey_columns['location_area'])
            
            # Get registration date from RecordedDate if available
            registration_date = self._safe_date(row.get('RecordedDate', ''))
            
            return Participant(
                participant_id=str(participant_id),
                age=age,
                gender=gender,
                ethnicity=ethnicity,
                education=education,
                employment=employment,
                income=income,
                household_size=household_size,
                location_area=location_area,
                registration_date=registration_date
            )
            
        except Exception as e:
            self.processing_stats['data_quality_issues'].append(f"Participant extraction error: {e}")
            return None
    
    def _extract_biweekly_data(self, row: pd.Series) -> Optional[BiweeklyResponse]:
        """Extract biweekly survey response data"""
        
        try:
            # Find response and participant IDs
            response_id = self._find_value(row, self.biweekly_survey_columns['response_id'])
            participant_id = self._find_value(row, self.biweekly_survey_columns['participant_id'])
            
            if not response_id or not participant_id:
                return None
            
            # Extract survey responses
            happiness_score = self._safe_float(self._find_value(row, self.biweekly_survey_columns['happiness_score']))
            stress_level = self._safe_float(self._find_value(row, self.biweekly_survey_columns['stress_level']))
            life_satisfaction = self._safe_float(self._find_value(row, self.biweekly_survey_columns['life_satisfaction']))
            health_rating = self._safe_float(self._find_value(row, self.biweekly_survey_columns['health_rating']))
            
            activity_level = self._find_value(row, self.biweekly_survey_columns['activity_level'])
            places_visited = self._safe_int(self._find_value(row, self.biweekly_survey_columns['places_visited']))
            social_interactions = self._safe_int(self._find_value(row, self.biweekly_survey_columns['social_interactions']))
            green_space_time = self._safe_float(self._find_value(row, self.biweekly_survey_columns['green_space_time']))
            transport_modes = self._find_value(row, self.biweekly_survey_columns['transport_modes'])
            
            # Check for location data
            has_location_data = 'DECRYPTED:' in str(row.get('Q18', ''))
            location_points_count = None
            if has_location_data:
                # Extract count from "DECRYPTED: X location points"
                location_text = str(row.get('Q18', ''))
                match = re.search(r'DECRYPTED: (\d+) location points', location_text)
                if match:
                    location_points_count = int(match.group(1))
            
            # Get submission date
            submission_date = self._safe_date(row.get('RecordedDate', ''))
            
            return BiweeklyResponse(
                response_id=str(response_id),
                participant_id=str(participant_id),
                submission_date=submission_date,
                happiness_score=happiness_score,
                stress_level=stress_level,
                life_satisfaction=life_satisfaction,
                health_rating=health_rating,
                activity_level=activity_level,
                places_visited=places_visited,
                social_interactions=social_interactions,
                green_space_time=green_space_time,
                transport_modes=transport_modes,
                has_location_data=has_location_data,
                location_points_count=location_points_count
            )
            
        except Exception as e:
            self.processing_stats['data_quality_issues'].append(f"Biweekly data extraction error: {e}")
            return None
    
    def _extract_consent_data(self, row: pd.Series) -> Optional[ConsentRecord]:
        """Extract consent form data"""
        
        try:
            # Find consent and participant IDs
            consent_id = row.get('ResponseId', '')
            participant_id = row.get('Q1', '')  # Assuming Q1 is participant ID
            
            if not consent_id or not participant_id:
                return None
            
            # Extract consent details (adjust column names based on actual survey)
            data_sharing_approved = self._safe_bool(row.get('Q2', False))
            location_sharing_approved = self._safe_bool(row.get('Q3', False))
            research_contact_approved = self._safe_bool(row.get('Q4', False))
            
            consent_date = self._safe_date(row.get('RecordedDate', ''))
            
            return ConsentRecord(
                consent_id=str(consent_id),
                participant_id=str(participant_id),
                consent_date=consent_date,
                consent_type='initial_consent',
                data_sharing_approved=data_sharing_approved,
                location_sharing_approved=location_sharing_approved,
                research_contact_approved=research_contact_approved
            )
            
        except Exception as e:
            self.processing_stats['data_quality_issues'].append(f"Consent data extraction error: {e}")
            return None
    
    def _extract_location_data(self, row: pd.Series) -> Optional[LocationTrack]:
        """Extract GPS location tracking data"""
        
        try:
            # Required fields
            participant_id = row.get('response_id', '')
            response_id = row.get('response_id', '')
            timestamp = row.get('timestamp', '')
            latitude = self._safe_float(row.get('latitude', ''))
            longitude = self._safe_float(row.get('longitude', ''))
            
            if not all([participant_id, response_id, timestamp, latitude, longitude]):
                return None
            
            # Optional fields
            accuracy = self._safe_float(row.get('accuracy', ''))
            altitude = self._safe_float(row.get('altitude', ''))
            speed = self._safe_float(row.get('speed', ''))
            heading = self._safe_float(row.get('heading', ''))
            
            # Parse timestamp for temporal analysis
            dt = self._parse_timestamp(timestamp)
            date = dt.date().isoformat() if dt else None
            time = dt.time().isoformat() if dt else None
            day_of_week = dt.strftime('%A') if dt else None
            hour_of_day = dt.hour if dt else None
            
            # Generate unique track ID
            track_id = f"{participant_id}_{timestamp}"
            
            return LocationTrack(
                track_id=track_id,
                participant_id=str(participant_id),
                response_id=str(response_id),
                timestamp=timestamp,
                latitude=latitude,
                longitude=longitude,
                accuracy=accuracy,
                altitude=altitude,
                speed=speed,
                heading=heading,
                date=date,
                time=time,
                day_of_week=day_of_week,
                hour_of_day=hour_of_day
            )
            
        except Exception as e:
            self.processing_stats['data_quality_issues'].append(f"Location data extraction error: {e}")
            return None
    
    def _find_files(self, filenames: List[str]) -> List[str]:
        """Find files matching any of the given filenames"""
        found_files = []
        for filename in filenames:
            file_path = os.path.join(self.input_dir, filename)
            if os.path.exists(file_path):
                found_files.append(file_path)
        return found_files
    
    def _find_value(self, row: pd.Series, column_names: List[str]) -> Optional[str]:
        """Find value from row using multiple possible column names"""
        for col_name in column_names:
            if col_name in row.index and pd.notna(row[col_name]):
                value = str(row[col_name]).strip()
                if value and value.lower() not in ['', 'nan', 'none', 'null']:
                    return value
        return None
    
    def _safe_int(self, value: Optional[str]) -> Optional[int]:
        """Safely convert string to integer"""
        if not value:
            return None
        try:
            return int(float(value))  # Handle "25.0" format
        except (ValueError, TypeError):
            return None
    
    def _safe_float(self, value: Optional[str]) -> Optional[float]:
        """Safely convert string to float"""
        if not value:
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None
    
    def _safe_bool(self, value: Any) -> bool:
        """Safely convert value to boolean"""
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.lower() in ['true', 'yes', '1', 'on', 'agree']
        if isinstance(value, (int, float)):
            return value > 0
        return False
    
    def _safe_date(self, value: str) -> Optional[str]:
        """Safely parse and format date"""
        if not value:
            return None
        try:
            # Try different date formats
            for fmt in ['%Y-%m-%d %H:%M:%S', '%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y']:
                try:
                    dt = datetime.strptime(str(value), fmt)
                    return dt.date().isoformat()
                except ValueError:
                    continue
        except:
            pass
        return None
    
    def _parse_timestamp(self, timestamp: str) -> Optional[datetime]:
        """Parse timestamp string to datetime object"""
        if not timestamp:
            return None
        try:
            # Try ISO format first
            return datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
        except:
            try:
                # Try other common formats
                return datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
            except:
                return None
    
    def _generate_output_files(self) -> None:
        """Generate all output files in different formats"""
        
        print(f"📁 Generating structured data files...")
        
        # Convert dataclasses to dictionaries
        participants_data = [asdict(p) for p in self.participants]
        biweekly_data = [asdict(r) for r in self.biweekly_responses]
        consent_data = [asdict(c) for c in self.consent_records]
        location_data = [asdict(t) for t in self.location_tracks]
        
        # Generate CSV files
        self._save_csv('participants.csv', participants_data)
        self._save_csv('biweekly_responses.csv', biweekly_data)
        self._save_csv('consent_records.csv', consent_data)
        self._save_csv('location_tracks.csv', location_data)
        
        # Generate JSON files
        self._save_json('participants.json', participants_data)
        self._save_json('biweekly_responses.json', biweekly_data)
        self._save_json('consent_records.json', consent_data)
        self._save_json('location_tracks.json', location_data)
        
        print(f"✅ Generated structured data files in {self.output_dir}")
    
    def _save_csv(self, filename: str, data: List[Dict]) -> None:
        """Save data as CSV file"""
        if not data:
            return
        
        filepath = os.path.join(self.output_dir, filename)
        df = pd.DataFrame(data)
        df.to_csv(filepath, index=False)
        print(f"   📄 {filename}: {len(data)} records")
    
    def _save_json(self, filename: str, data: List[Dict]) -> None:
        """Save data as JSON file"""
        if not data:
            return
        
        filepath = os.path.join(self.output_dir, filename)
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2, default=str)
    
    def generate_sqlite_database(self, db_path: str) -> None:
        """Generate SQLite database with all structured tables"""
        
        print(f"🗄️ Creating SQLite database: {db_path}")
        
        conn = sqlite3.connect(db_path)
        
        # Create tables and insert data
        if self.participants:
            df = pd.DataFrame([asdict(p) for p in self.participants])
            df.to_sql('participants', conn, if_exists='replace', index=False)
            print(f"   📋 participants table: {len(self.participants)} records")
        
        if self.biweekly_responses:
            df = pd.DataFrame([asdict(r) for r in self.biweekly_responses])
            df.to_sql('biweekly_responses', conn, if_exists='replace', index=False)
            print(f"   📊 biweekly_responses table: {len(self.biweekly_responses)} records")
        
        if self.consent_records:
            df = pd.DataFrame([asdict(c) for c in self.consent_records])
            df.to_sql('consent_records', conn, if_exists='replace', index=False)
            print(f"   📋 consent_records table: {len(self.consent_records)} records")
        
        if self.location_tracks:
            df = pd.DataFrame([asdict(t) for t in self.location_tracks])
            df.to_sql('location_tracks', conn, if_exists='replace', index=False)
            print(f"   📍 location_tracks table: {len(self.location_tracks)} records")
        
        conn.close()
        print(f"✅ SQLite database created successfully")
    
    def _generate_summary_report(self) -> None:
        """Generate data processing summary report"""
        
        report_path = os.path.join(self.output_dir, 'data_summary.md')
        
        with open(report_path, 'w') as f:
            f.write("# Structured Data Tables Summary\n\n")
            f.write(f"Generated on: {datetime.now().isoformat()}\n\n")
            
            # Processing statistics
            f.write("## Processing Statistics\n\n")
            f.write(f"- Files processed: {self.processing_stats['files_processed']}\n")
            f.write(f"- Participants found: {self.processing_stats['participants_found']}\n")
            f.write(f"- Survey responses: {self.processing_stats['responses_processed']}\n")
            f.write(f"- Location points: {self.processing_stats['location_points']}\n")
            f.write(f"- Data quality issues: {len(self.processing_stats['data_quality_issues'])}\n\n")
            
            # Table summaries
            f.write("## Data Tables\n\n")
            
            f.write("### 1. Participants Table\n")
            f.write(f"- Records: {len(self.participants)}\n")
            f.write("- Contains: Demographics, registration info, consent status\n")
            f.write("- Key fields: participant_id, age, gender, ethnicity, education\n\n")
            
            f.write("### 2. Biweekly Responses Table\n")
            f.write(f"- Records: {len(self.biweekly_responses)}\n")
            f.write("- Contains: Wellbeing measures, activity data, temporal info\n")
            f.write("- Key fields: response_id, participant_id, happiness_score, location_data\n\n")
            
            f.write("### 3. Consent Records Table\n")
            f.write(f"- Records: {len(self.consent_records)}\n")
            f.write("- Contains: Data sharing permissions, legal compliance\n")
            f.write("- Key fields: consent_id, participant_id, data_sharing_approved\n\n")
            
            f.write("### 4. Location Tracks Table\n")
            f.write(f"- Records: {len(self.location_tracks)}\n")
            f.write("- Contains: GPS coordinates, movement data, temporal analysis\n")
            f.write("- Key fields: track_id, participant_id, latitude, longitude, timestamp\n\n")
            
            # Data quality issues
            if self.processing_stats['data_quality_issues']:
                f.write("## Data Quality Issues\n\n")
                for issue in self.processing_stats['data_quality_issues']:
                    f.write(f"- {issue}\n")
                f.write("\n")
            
            # Usage examples
            f.write("## Usage Examples\n\n")
            f.write("### Python/Pandas\n")
            f.write("```python\n")
            f.write("import pandas as pd\n\n")
            f.write("# Load structured data\n")
            f.write("participants = pd.read_csv('participants.csv')\n")
            f.write("responses = pd.read_csv('biweekly_responses.csv')\n")
            f.write("locations = pd.read_csv('location_tracks.csv')\n\n")
            f.write("# Example analysis\n")
            f.write("happiness_by_age = responses.groupby('participant_id')['happiness_score'].mean()\n")
            f.write("```\n\n")
            
            f.write("### SQLite\n")
            f.write("```sql\n")
            f.write("-- Load database\n")
            f.write("sqlite3 research_data.db\n\n")
            f.write("-- Example query\n")
            f.write("SELECT p.age, AVG(r.happiness_score) as avg_happiness\n")
            f.write("FROM participants p\n")
            f.write("JOIN biweekly_responses r ON p.participant_id = r.participant_id\n")
            f.write("GROUP BY p.age;\n")
            f.write("```\n")
        
        print(f"📋 Summary report saved: {report_path}")


def main():
    parser = argparse.ArgumentParser(
        description='Generate structured, analysis-ready tables from decrypted survey data',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    %(prog)s --input ./data/decrypted                Process all decrypted files
    %(prog)s --input ./data/decrypted --database research.db   Create SQLite database
    %(prog)s --input ./data/decrypted --summary                Generate summary report
        """
    )
    
    # Input/output options
    parser.add_argument('--input', default='./data/decrypted',
                       help='Input directory with decrypted CSV files (default: ./data/decrypted)')
    
    parser.add_argument('--output', default='./data/structured',
                       help='Output directory for structured tables (default: ./data/structured)')
    
    # Output format options
    parser.add_argument('--database', 
                       help='Create SQLite database with given filename')
    
    parser.add_argument('--summary', action='store_true',
                       help='Generate detailed summary report')
    
    parser.add_argument('--json-only', action='store_true',
                       help='Generate only JSON files (skip CSV)')
    
    parser.add_argument('--csv-only', action='store_true',
                       help='Generate only CSV files (skip JSON)')
    
    args = parser.parse_args()
    
    # Create generator
    generator = StructuredDataTablesGenerator(args.input, args.output)
    
    # Process all data
    success = generator.process_all_data()
    
    if not success:
        print("❌ Failed to process data")
        return 1
    
    # Generate SQLite database if requested
    if args.database:
        db_path = args.database
        if not db_path.endswith('.db'):
            db_path += '.db'
        generator.generate_sqlite_database(db_path)
    
    # Print final summary
    print(f"\n{'='*60}")
    print("📊 STRUCTURED DATA TABLES SUMMARY")
    print(f"{'='*60}")
    print(f"Participants: {len(generator.participants)}")
    print(f"Survey responses: {len(generator.biweekly_responses)}")
    print(f"Consent records: {len(generator.consent_records)}")
    print(f"Location points: {len(generator.location_tracks)}")
    print(f"Output directory: {args.output}")
    
    if generator.processing_stats['data_quality_issues']:
        print(f"⚠️ Data quality issues: {len(generator.processing_stats['data_quality_issues'])}")
        print("   See data_summary.md for details")
    
    print(f"\n🎉 Structured data tables created successfully!")
    return 0


if __name__ == '__main__':
    # Check dependencies
    try:
        import pandas as pd
    except ImportError:
        print("❌ Missing required dependency: pandas")
        print("Install with: pip install pandas")
        sys.exit(1)
    
    sys.exit(main())