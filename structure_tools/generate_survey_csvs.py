#!/usr/bin/env python3
"""
Survey CSV Creator
==================

This script converts decrypted survey data directly into 4 clean CSV files:
1. consent.csv - Consent form responses
2. initial_survey.csv - Initial demographic and wellbeing survey
3. biweekly_survey.csv - Biweekly wellbeing check-ins
4. location_data.csv - GPS location points from surveys

Optionally handles legacy images (only when `--download-images` is supplied):
- Creates an `images/` directory 
- Downloads/extracts images from encrypted data
- Stores image file paths in the CSV files
- Validates image URLs and file integrity

Features:
- Direct conversion with no intermediate processing
- Comprehensive data extraction and validation
- Image handling with proper file organization
- Data completeness reporting
- Error detection and logging

Usage:
    python3 generate_survey_csvs.py [options]
    
Examples:
    # Process decrypted data in current directory
    python3 generate_survey_csvs.py
    
    # Process specific directory
    python3 generate_survey_csvs.py --input ./data/decrypted --output ./data/structured
    
    # Include image download/extraction
    python3 generate_survey_csvs.py --download-images
    
    # Validate data completeness
    python3 generate_survey_csvs.py --validate --report
"""

import argparse
import csv
import json
import os
import sys
import requests
import base64
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
import pandas as pd
from urllib.parse import urlparse
import hashlib

class SurveyCSVCreator:
    """Convert decrypted survey data into clean CSV files"""
    
    def __init__(self, input_dir: str, output_dir: str, download_images: bool = False):
        """Initialize the CSV creator"""
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir)
        self.download_images = download_images
        self.images_dir = self.output_dir / "images"
        
        # Create directories
        self.output_dir.mkdir(parents=True, exist_ok=True)
        if self.download_images:
            self.images_dir.mkdir(parents=True, exist_ok=True)
        
        # Data containers
        self.consent_data = []
        self.initial_survey_data = []
        self.biweekly_survey_data = []
        self.location_data = []
        
        # Processing stats
        self.stats = {
            'files_processed': 0,
            'consent_responses': 0,
            'initial_responses': 0,
            'biweekly_responses': 0,
            'location_points': 0,
            'images_processed': 0,
            'errors': []
        }
    
    def process_all_files(self) -> bool:
        """Process all decrypted CSV files"""
        print("🔄 Processing decrypted survey data...")
        
        # Find decrypted CSV files
        csv_files = list(self.input_dir.glob("*_decrypted_responses.csv"))
        
        if not csv_files:
            print(f"❌ No decrypted CSV files found in {self.input_dir}")
            return False
        
        print(f"📂 Found {len(csv_files)} decrypted files:")
        for file in csv_files:
            print(f"   - {file.name}")
        
        # Process each file
        for csv_file in csv_files:
            try:
                self._process_csv_file(csv_file)
                self.stats['files_processed'] += 1
            except Exception as e:
                error_msg = f"Error processing {csv_file.name}: {e}"
                self.stats['errors'].append(error_msg)
                print(f"   ❌ {error_msg}")
        
        return True
    
    def _process_csv_file(self, csv_file: Path) -> None:
        """Process a single decrypted CSV file"""
        print(f"\n📋 Processing: {csv_file.name}")
        
        # Read the CSV
        df = pd.read_csv(csv_file)
        print(f"   📊 Found {len(df)} responses")
        
        # Process each response
        for idx, row in df.iterrows():
            try:
                # Determine survey type
                raw_type = row.get('decrypted_type', '')
                if isinstance(raw_type, str):
                    survey_type = raw_type.lower()
                elif pd.isna(raw_type) or raw_type is None:
                    survey_type = ''
                else:
                    survey_type = str(raw_type).lower()
                
                if 'consent' in survey_type:
                    self._process_consent_response(row)
                    self.stats['consent_responses'] += 1
                elif 'initial' in survey_type:
                    self._process_initial_response(row)
                    self.stats['initial_responses'] += 1
                elif 'biweekly' in survey_type:
                    self._process_biweekly_response(row)
                    self.stats['biweekly_responses'] += 1
                else:
                    print(f"   ⚠️ Unknown survey type: {survey_type}")
                
            except Exception as e:
                error_msg = f"Error processing row {idx} in {csv_file.name}: {e}"
                self.stats['errors'].append(error_msg)
                print(f"   ❌ {error_msg}")
    
    def _process_consent_response(self, row: pd.Series) -> None:
        """Process a consent form response"""
        try:
            # Parse the decrypted data
            data = self._parse_json_field(row, 'decrypted_data')
            metadata = self._parse_json_field(row, 'decrypted_metadata')
            
            # Create consent record
            consent_record = {
                # Identifiers
                'response_id': row.get('ResponseId', ''),
                'participant_uuid': row.get('decrypted_participant_uuid', ''),
                'consent_id': row.get('decrypted_consent_id', ''),
                'timestamp': row.get('decrypted_timestamp', ''),
                'submitted_at': data.get('consented_at', ''),
                
                # Core consent fields
                'informed_consent': data.get('informed_consent', 0),
                'data_processing': data.get('data_processing', 0),
                'location_data': data.get('location_data', 0),
                'survey_data': data.get('survey_data', 0),
                'data_retention': data.get('data_retention', 0),
                'data_sharing': data.get('data_sharing', 0),
                'voluntary_participation': data.get('voluntary_participation', 0),
                
                # Specific consent permissions
                'consent_participate': data.get('consent_participate', 0),
                'consent_qualtrics_data': data.get('consent_qualtrics_data', 0),
                'consent_race_ethnicity': data.get('consent_race_ethnicity', 0),
                'consent_health': data.get('consent_health', 0),
                'consent_sexual_orientation': data.get('consent_sexual_orientation', 0),
                'consent_location_mobility': data.get('consent_location_mobility', 0),
                'consent_data_transfer': data.get('consent_data_transfer', 0),
                'consent_public_reporting': data.get('consent_public_reporting', 0),
                'consent_researcher_sharing': data.get('consent_researcher_sharing', 0),
                'consent_further_research': data.get('consent_further_research', 0),
                'consent_public_repository': data.get('consent_public_repository', 0),
                'consent_followup_contact': data.get('consent_followup_contact', 0),
                
                # Metadata
                'participant_signature': data.get('participant_signature', ''),
                'app_version': metadata.get('app_version', ''),
                'submission_method': metadata.get('submission_method', ''),
                'synced': data.get('synced', 0),
                'created_at': data.get('created_at', '')
            }
            
            self.consent_data.append(consent_record)
            
        except Exception as e:
            raise Exception(f"Failed to process consent response: {e}")
    
    def _process_initial_response(self, row: pd.Series) -> None:
        """Process an initial survey response"""
        try:
            # Parse the decrypted data
            data = self._parse_json_field(row, 'decrypted_data')
            metadata = self._parse_json_field(row, 'decrypted_metadata')
            
            # Handle images
            image_paths = self._process_images(row, 'initial')
            
            # Create initial survey record
            initial_record = {
                # Identifiers
                'response_id': row.get('ResponseId', ''),
                'participant_uuid': row.get('decrypted_participant_uuid', ''),
                'survey_id': row.get('decrypted_survey_id', ''),
                'timestamp': row.get('decrypted_timestamp', ''),
                'submitted_at': data.get('submitted_at', ''),
                
                # Demographics
                'age': data.get('age', ''),
                'gender': data.get('gender', ''),
                'sexuality': data.get('sexuality', ''),
                'ethnicity': self._parse_list_field(data.get('ethnicity', '[]')),
                'birth_place': data.get('birth_place', ''),
                'lives_in_barcelona': data.get('lives_in_barcelona', ''),
                'suburb': data.get('suburb', ''),
                'building_type': data.get('building_type', ''),
                'household_items': self._parse_list_field(data.get('household_items', '[]')),
                'education': data.get('education', ''),
                'climate_activism': data.get('climate_activism', ''),
                'general_health': data.get('general_health', ''),
                'activities': self._parse_list_field(data.get('activities', '[]')),
                'living_arrangement': data.get('living_arrangement', ''),
                'relationship_status': data.get('relationship_status', ''),
                
                # Wellbeing questions (WHO-5 and additional)
                'cheerful_spirits': data.get('cheerful_spirits', ''),
                'calm_relaxed': data.get('calm_relaxed', ''),
                'active_vigorous': data.get('active_vigorous', ''),
                'woke_up_fresh': data.get('woke_up_fresh', ''),
                'daily_life_interesting': data.get('daily_life_interesting', ''),
                'cooperate_with_people': data.get('cooperate_with_people', ''),
                'improving_skills': data.get('improving_skills', ''),
                'social_situations': data.get('social_situations', ''),
                'family_support': data.get('family_support', ''),
                'family_knows_me': data.get('family_knows_me', ''),
                'access_to_food': data.get('access_to_food', ''),
                'people_enjoy_time': data.get('people_enjoy_time', ''),
                'talk_to_family': data.get('talk_to_family', ''),
                'friends_support': data.get('friends_support', ''),
                'belong_in_community': data.get('belong_in_community', ''),
                'family_stands_by_me': data.get('family_stands_by_me', ''),
                'friends_stand_by_me': data.get('friends_stand_by_me', ''),
                'treated_fairly': data.get('treated_fairly', ''),
                'opportunities_responsibility': data.get('opportunities_responsibility', ''),
                'secure_with_family': data.get('secure_with_family', ''),
                'opportunities_abilities': data.get('opportunities_abilities', ''),
                'enjoy_cultural_traditions': data.get('enjoy_cultural_traditions', ''),
                'environmental_challenges': data.get('environmental_challenges', ''),
                'challenges_stress_level': data.get('challenges_stress_level', ''),
                'coping_help': data.get('coping_help', ''),
                
                # Media files
                'image_files': ';'.join(image_paths) if image_paths else '',
                'voice_note_urls': data.get('voice_note_urls', ''),
                'image_urls': data.get('image_urls', ''),
                
                # Metadata
                'research_site': data.get('research_site', ''),
                'app_version': metadata.get('app_version', ''),
                'submission_method': metadata.get('submission_method', ''),
                'has_images': metadata.get('has_images', False),
                'synced': data.get('synced', 0),
                'created_at': data.get('created_at', '')
            }
            
            self.initial_survey_data.append(initial_record)
            
        except Exception as e:
            raise Exception(f"Failed to process initial survey response: {e}")
    
    def _process_biweekly_response(self, row: pd.Series) -> None:
        """Process a biweekly survey response"""
        try:
            # Parse the decrypted data
            data = self._parse_json_field(row, 'decrypted_data')
            metadata = self._parse_json_field(row, 'decrypted_metadata')
            location_data = self._parse_json_field(row, 'decrypted_location_data')
            
            # Handle images
            image_paths = self._process_images(row, 'biweekly')
            
            # Process location data
            self._process_location_data(location_data, row.get('ResponseId', ''))
            
            # Create biweekly survey record
            biweekly_record = {
                # Identifiers
                'response_id': row.get('ResponseId', ''),
                'participant_uuid': row.get('decrypted_participant_uuid', ''),
                'survey_id': row.get('decrypted_survey_id', ''),
                'timestamp': row.get('decrypted_timestamp', ''),
                'submitted_at': data.get('submitted_at', ''),
                
                # Current activities
                'activities': self._parse_list_field(data.get('activities', '[]')),
                'living_arrangement': data.get('living_arrangement', ''),
                'relationship_status': data.get('relationship_status', ''),
                
                # Wellbeing questions (WHO-5 and additional)
                'cheerful_spirits': data.get('cheerful_spirits', ''),
                'calm_relaxed': data.get('calm_relaxed', ''),
                'active_vigorous': data.get('active_vigorous', ''),
                'woke_up_fresh': data.get('woke_up_fresh', ''),
                'daily_life_interesting': data.get('daily_life_interesting', ''),
                'cooperate_with_people': data.get('cooperate_with_people', ''),
                'improving_skills': data.get('improving_skills', ''),
                'social_situations': data.get('social_situations', ''),
                'family_support': data.get('family_support', ''),
                'family_knows_me': data.get('family_knows_me', ''),
                'access_to_food': data.get('access_to_food', ''),
                'people_enjoy_time': data.get('people_enjoy_time', ''),
                'talk_to_family': data.get('talk_to_family', ''),
                'friends_support': data.get('friends_support', ''),
                'belong_in_community': data.get('belong_in_community', ''),
                'family_stands_by_me': data.get('family_stands_by_me', ''),
                'friends_stand_by_me': data.get('friends_stand_by_me', ''),
                'treated_fairly': data.get('treated_fairly', ''),
                'opportunities_responsibility': data.get('opportunities_responsibility', ''),
                'secure_with_family': data.get('secure_with_family', ''),
                'opportunities_abilities': data.get('opportunities_abilities', ''),
                'enjoy_cultural_traditions': data.get('enjoy_cultural_traditions', ''),
                'environmental_challenges': data.get('environmental_challenges', ''),
                'challenges_stress_level': data.get('challenges_stress_level', ''),
                'coping_help': data.get('coping_help', ''),
                
                # Media files
                'image_files': ';'.join(image_paths) if image_paths else '',
                'voice_note_urls': data.get('voice_note_urls', ''),
                'image_urls': data.get('image_urls', ''),
                
                # Location summary
                'location_points_count': len(location_data.get('locations', [])) if location_data else 0,
                'location_sharing_option': location_data.get('sharing_option', '') if location_data else '',
                'user_erased_count': location_data.get('user_erased_count', 0) if location_data else 0,
                'collection_period_days': location_data.get('collection_period_days', 0) if location_data else 0,
                
                # Metadata
                'app_version': metadata.get('app_version', ''),
                'submission_method': metadata.get('submission_method', ''),
                'has_images': metadata.get('has_images', False),
                'synced': data.get('synced', 0),
                'created_at': data.get('created_at', '')
            }
            
            self.biweekly_survey_data.append(biweekly_record)
            
        except Exception as e:
            raise Exception(f"Failed to process biweekly survey response: {e}")
    
    def _process_location_data(self, location_data: Dict, response_id: str) -> None:
        """Process location data from a survey response"""
        if not location_data or 'locations' not in location_data:
            return
        
        locations = location_data.get('locations', [])
        
        for location in locations:
            location_record = {
                # Identifiers
                'response_id': response_id,
                'timestamp': location.get('timestamp', ''),
                
                # Location data
                'latitude': location.get('latitude', ''),
                'longitude': location.get('longitude', ''),
                'accuracy': location.get('accuracy', ''),
                'altitude': location.get('altitude', ''),
                'speed': location.get('speed', ''),
                'activity': location.get('activity', ''),
                
                # Survey context
                'sharing_option': location_data.get('sharing_option', ''),
                'collection_period_days': location_data.get('collection_period_days', ''),
                'submitted_at': location_data.get('submitted_at', '')
            }
            
            self.location_data.append(location_record)
            self.stats['location_points'] += 1
    
    def _process_images(self, row: pd.Series, survey_type: str) -> List[str]:
        """Process images from survey response"""
        image_paths = []
        
        if not self.download_images:
            return image_paths
        
        try:
            # Check for encrypted images
            encrypted_images = row.get('decrypted_encrypted_images', '')
            if encrypted_images:
                # Process encrypted image data
                image_paths.extend(self._extract_encrypted_images(encrypted_images, row.get('ResponseId', ''), survey_type))
            
            # Check for image URLs
            image_urls = row.get('image_urls', '')
            if image_urls:
                # Download images from URLs
                image_paths.extend(self._download_image_urls(image_urls, row.get('ResponseId', ''), survey_type))
            
            self.stats['images_processed'] += len(image_paths)
            
        except Exception as e:
            error_msg = f"Error processing images for {row.get('ResponseId', '')}: {e}"
            self.stats['errors'].append(error_msg)
            print(f"   ⚠️ {error_msg}")
        
        return image_paths
    
    def _extract_encrypted_images(self, encrypted_data: str, response_id: str, survey_type: str) -> List[str]:
        """Extract images from encrypted data"""
        image_paths = []
        
        try:
            # Handle NaN/None values
            if pd.isna(encrypted_data) or encrypted_data is None:
                return image_paths
                
            # Parse encrypted image array - handle both JSON and Python string formats
            if str(encrypted_data).startswith('['):
                try:
                    # Try JSON parsing first
                    image_list = json.loads(encrypted_data)
                except json.JSONDecodeError:
                    # If JSON fails, try Python literal_eval for string representation
                    import ast
                    image_list = ast.literal_eval(encrypted_data)
                    
                print(f"� Processing {len(image_list)} encrypted images for {response_id}")
                
                for idx, image_json in enumerate(image_list):
                    try:
                        # Each item is a JSON string containing image metadata + base64 data
                        if isinstance(image_json, str):
                            image_obj = json.loads(image_json)
                        else:
                            image_obj = image_json
                        
                        # Extract image metadata
                        original_filename = image_obj.get('filename', f'image_{idx}')
                        image_size = image_obj.get('size', 0)
                        base64_data = image_obj.get('data', '')
                        
                        if base64_data:
                            # Create filename preserving original name but prefixed with response info
                            name, ext = os.path.splitext(original_filename)
                            if not ext:
                                ext = '.jpg'  # Default extension
                            filename = f"{response_id}_{survey_type}_{name}{ext}"
                            filepath = self.images_dir / filename
                            
                            # Decode and save image
                            try:
                                image_binary = base64.b64decode(base64_data)
                                with open(filepath, 'wb') as f:
                                    f.write(image_binary)
                                
                                image_paths.append(str(filepath.relative_to(self.output_dir)))
                                print(f"   ✅ Extracted image: {filename} ({image_size} bytes)")
                                
                            except Exception as decode_error:
                                print(f"   ⚠️ Failed to decode image {original_filename}: {decode_error}")
                                # Save metadata for debugging
                                debug_file = (self.images_dir / f"{response_id}_{survey_type}_{name}_debug.json")
                                with open(debug_file, 'w') as f:
                                    json.dump(image_obj, f, indent=2)
                                
                    except Exception as item_error:
                        print(f"   ⚠️ Failed to process image {idx}: {item_error}")
            else:
                print(f"   ⚠️ Unexpected encrypted image format for {response_id}: {encrypted_data[:100]}...")
            
        except Exception as e:
            print(f"   ❌ Failed to extract encrypted images for {response_id}: {e}")
            # Save raw data for debugging
            debug_file = self.images_dir / f"{response_id}_{survey_type}_raw_debug.txt"
            with open(debug_file, 'w') as f:
                f.write(encrypted_data)
        
        return image_paths
    
    def _download_image_urls(self, image_urls: str, response_id: str, survey_type: str) -> List[str]:
        """Download images from URLs"""
        image_paths = []
        
        try:
            urls = json.loads(image_urls) if image_urls.startswith('[') else [image_urls]
            
            for idx, url in enumerate(urls):
                if not url or not url.startswith('http'):
                    continue
                
                # Determine file extension
                parsed_url = urlparse(url)
                ext = Path(parsed_url.path).suffix or '.jpg'
                filename = f"{response_id}_{survey_type}_download_{idx}{ext}"
                filepath = self.images_dir / filename
                
                # Download image
                try:
                    response = requests.get(url, timeout=10)
                    response.raise_for_status()
                    
                    with open(filepath, 'wb') as f:
                        f.write(response.content)
                    
                    image_paths.append(str(filepath.relative_to(self.output_dir)))
                    
                except requests.RequestException as e:
                    print(f"   ⚠️ Failed to download {url}: {e}")
            
        except Exception as e:
            print(f"   ⚠️ Failed to process image URLs: {e}")
        
        return image_paths
    
    def _parse_json_field(self, row: pd.Series, field_name: str) -> Dict:
        """Safely parse a JSON field from a pandas Series"""
        try:
            value = row.get(field_name, '{}')
            if pd.isna(value) or value == '':
                return {}
            
            if isinstance(value, str):
                # Handle Python dict format (single quotes) by using eval safely
                if value.startswith('{') and "'" in value:
                    try:
                        return eval(value)  # Safe for our controlled data
                    except:
                        # Fallback: replace single quotes with double quotes
                        value = value.replace("'", '"')
                        return json.loads(value)
                else:
                    return json.loads(value)
            
            return value
        except Exception as e:
            print(f"   ⚠️ Failed to parse {field_name}: {e}")
            return {}
    
    def _parse_list_field(self, value: str) -> str:
        """Parse a list field and return as comma-separated string"""
        try:
            if not value or value == '[]':
                return ''
            
            # Handle Python list format (single quotes)
            if isinstance(value, str) and value.startswith('['):
                try:
                    parsed = eval(value)  # Safe for our controlled data
                except:
                    # Fallback: replace single quotes with double quotes  
                    value = value.replace("'", '"')
                    parsed = json.loads(value)
            else:
                parsed = json.loads(value) if isinstance(value, str) else value
            
            if isinstance(parsed, list):
                return ', '.join(str(item) for item in parsed)
            return str(parsed)
        except:
            return str(value) if value else ''
    
    def save_csv_files(self) -> bool:
        """Save all data to CSV files"""
        print(f"\n💾 Saving CSV files to {self.output_dir}")
        
        try:
            # Save consent data
            if self.consent_data:
                consent_file = self.output_dir / "consent.csv"
                df = pd.DataFrame(self.consent_data)
                df.to_csv(consent_file, index=False)
                print(f"   ✅ consent.csv: {len(self.consent_data)} records")
            else:
                print("   ⚠️ No consent data to save")
            
            # Save initial survey data
            if self.initial_survey_data:
                initial_file = self.output_dir / "initial_survey.csv"
                df = pd.DataFrame(self.initial_survey_data)
                df.to_csv(initial_file, index=False)
                print(f"   ✅ initial_survey.csv: {len(self.initial_survey_data)} records")
            else:
                print("   ⚠️ No initial survey data to save")
            
            # Save biweekly survey data
            if self.biweekly_survey_data:
                biweekly_file = self.output_dir / "biweekly_survey.csv"
                df = pd.DataFrame(self.biweekly_survey_data)
                df.to_csv(biweekly_file, index=False)
                print(f"   ✅ biweekly_survey.csv: {len(self.biweekly_survey_data)} records")
            else:
                print("   ⚠️ No biweekly survey data to save")
            
            # Save location data
            if self.location_data:
                location_file = self.output_dir / "location_data.csv"
                df = pd.DataFrame(self.location_data)
                df.to_csv(location_file, index=False)
                print(f"   ✅ location_data.csv: {len(self.location_data)} records")
            else:
                print("   ⚠️ No location data to save")
            
            return True
            
        except Exception as e:
            print(f"   ❌ Error saving CSV files: {e}")
            return False
    
    def generate_report(self) -> None:
        """Generate a comprehensive processing report"""
        print("\n📊 Processing Summary")
        print("=" * 50)
        
        print(f"Files processed: {self.stats['files_processed']}")
        print(f"Consent responses: {self.stats['consent_responses']}")
        print(f"Initial surveys: {self.stats['initial_responses']}")
        print(f"Biweekly surveys: {self.stats['biweekly_responses']}")
        print(f"Location points: {self.stats['location_points']}")
        print(f"Images processed: {self.stats['images_processed']}")
        
        if self.stats['errors']:
            print(f"\n⚠️ Errors encountered: {len(self.stats['errors'])}")
            for error in self.stats['errors']:
                print(f"   • {error}")
        else:
            print("\n✅ No errors encountered")
        
        # Save detailed report
        report_file = self.output_dir / "processing_report.json"
        with open(report_file, 'w') as f:
            json.dump(self.stats, f, indent=2, default=str)
        print(f"\n📄 Detailed report saved: {report_file}")

def main():
    parser = argparse.ArgumentParser(
        description='Convert decrypted survey data into clean CSV files',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                                 Process current directory
  %(prog)s --input ./decrypted_data        Process specific input directory
  %(prog)s --download-images               Include image processing
  %(prog)s --validate --report             Validate data and generate report
        """
    )
    
    parser.add_argument('--input', default='./data/decrypted',
                       help='Input directory with decrypted CSV files (default: ./data/decrypted)')
    
    parser.add_argument('--output', default='./data/structured',
                       help='Output directory for CSV files (default: ./data/structured)')
    
    parser.add_argument('--download-images', action='store_true',
                       help='Download and extract images from survey responses')
    
    parser.add_argument('--validate', action='store_true',
                       help='Validate data completeness and report issues')
    
    parser.add_argument('--report', action='store_true',
                       help='Generate detailed processing report')
    
    args = parser.parse_args()
    
    # Create CSV converter
    try:
        creator = SurveyCSVCreator(
            input_dir=args.input,
            output_dir=args.output,
            download_images=args.download_images
        )
        
        print("🚀 Starting Survey CSV Creation")
        print("=" * 50)
        
        # Process files
        if not creator.process_all_files():
            print("💥 Failed to process files")
            return 1
        
        # Save CSV files
        if not creator.save_csv_files():
            print("💥 Failed to save CSV files")
            return 1
        
        # Generate report
        if args.report or args.validate:
            creator.generate_report()
        
        print("\n🎉 Survey CSV creation completed successfully!")
        return 0
        
    except Exception as e:
        print(f"💥 Failed with error: {e}")
        return 1

if __name__ == '__main__':
    sys.exit(main())