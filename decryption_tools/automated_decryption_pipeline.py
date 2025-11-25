#!/usr/bin/env python3
"""
Automated Decryption Pipeline for Qualt        # Survey type configurations
        self.survey_type_configs = {
            'initial': {
                'encrypted_data_column': 'encrypted_data',
                'location_column': None,  # Initial survey doesn't have location data
                'output_prefix': 'initial_decrypted'
            },
            'biweekly': {
                'encrypted_data_column': 'encrypted_data',
                'location_column': 'encrypted_data',  # Location data is in encrypted_data for biweekly
                'output_prefix': 'biweekly_decrypted'
            },
            'consent': {
                'encrypted_data_column': 'encrypted_data',
                'location_column': None,  # Consent form doesn't have location data
                'output_prefix': 'consent_decrypted'
            }
        }=====================================================

This script automatically processes downloaded Qualtrics data and decrypts all survey responses.
It integrates with the download_qualtrics_data.py script to create a complete data processing pipeline.

Features:
- Automatic detection of downloaded CSV files
- Batch decryption of all survey responses
- Structured output with separate files for different data types
- Progress tracking and error handling
- Integration with existing decryption tools

Usage:
    python3 automated_decryption_pipeline.py [options]
    
Examples:
    # Process all data in ./data directory
    python3 automated_decryption_pipeline.py --input ./data
    
    # Process specific survey file
    python3 automated_decryption_pipeline.py --file biweekly_survey_responses.csv
    
    # Full pipeline: download then decrypt
    python3 automated_decryption_pipeline.py --download-first --days 7
"""

import argparse
import json
import base64
import csv
import hashlib
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import subprocess

# Import existing decryption functionality
try:
    from cryptography.hazmat.primitives import serialization, hashes
    from cryptography.hazmat.primitives.asymmetric import rsa, padding
    from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
    from cryptography.hazmat.backends import default_backend
except ImportError:
    print("❌ Error: Required 'cryptography' library not found.")
    print("Please install it by running: pip install cryptography")
    print("Then run this script again.")
    sys.exit(1)

class AutomatedDecryptionPipeline:
    """Complete pipeline for downloading and decrypting Qualtrics survey data"""
    
    def __init__(self, private_key_path: str = '../pipeline_toolkit/secrets/private_key.pem'):
        self.private_key_path = private_key_path
        self.private_key = None
        self.results = {
            'processed_files': [],
            'decrypted_responses': 0,
            'location_points': 0,
            'errors': []
        }
        
        # Survey configurations based on survey_type column values
        # All data comes from a single CSV file but contains different survey types
        self.survey_type_configs = {
            'initial': {  # Initial demographics survey
                'encrypted_data_column': 'encrypted_data',
                'location_column': None,  # Initial survey doesn't have location data
                'output_prefix': 'initial_decrypted'
            },
            'biweekly': {  # Biweekly wellbeing survey
                'encrypted_data_column': 'encrypted_data',
                'location_column': 'encrypted_data',  # Location data in encrypted_data column
                'output_prefix': 'biweekly_decrypted'
            },
            'consent': {  # Consent form
                'encrypted_data_column': 'encrypted_data',
                'location_column': None,  # Consent form doesn't have location data
                'output_prefix': 'consent_decrypted'
            }
        }
        
        # Default file configuration for unified survey data
        self.default_csv_file = 'wellbeing_mapper_responses.csv'
    
    def load_private_key(self, password: Optional[str] = None) -> bool:
        """Load RSA private key for decryption"""
        try:
            if not os.path.exists(self.private_key_path):
                print(f"❌ Private key file not found: {self.private_key_path}")
                return False
                
            with open(self.private_key_path, 'rb') as key_file:
                if password:
                    password = password.encode('utf-8')
                
                self.private_key = serialization.load_pem_private_key(
                    key_file.read(),
                    password=password,
                    backend=default_backend()
                )
            
            print(f"✅ Successfully loaded private key from {self.private_key_path}")
            return True
            
        except Exception as e:
            print(f"❌ Error loading private key: {e}")
            self.results['errors'].append(f"Key loading error: {e}")
            return False
    
    def decrypt_aes_key(self, encrypted_key_b64: str) -> Optional[bytes]:
        """Decrypt AES key using RSA private key"""
        try:
            encrypted_key = base64.b64decode(encrypted_key_b64)
            
            # Try OAEP padding first (newer, more secure)
            try:
                decrypted_data = self.private_key.decrypt(
                    encrypted_key,
                    padding.OAEP(
                        mgf=padding.MGF1(algorithm=hashes.SHA256()),
                        algorithm=hashes.SHA256(),
                        label=None
                    )
                )
                # The decrypted data is a base64-encoded AES key string
                # We need to decode it to get the actual 32-byte key
                aes_key_base64 = decrypted_data.decode('utf-8')
                aes_key = base64.b64decode(aes_key_base64)
                return aes_key
            except Exception:
                # Fallback to PKCS1v15 padding (older format)
                decrypted_data = self.private_key.decrypt(
                    encrypted_key,
                    padding.PKCS1v15()
                )
                # The decrypted data is a base64-encoded AES key string
                # We need to decode it to get the actual 32-byte key
                aes_key_base64 = decrypted_data.decode('utf-8')
                aes_key = base64.b64decode(aes_key_base64)
                return aes_key
                
        except Exception as e:
            print(f"❌ Error decrypting AES key: {e}")
            return None
    
    def decrypt_location_data(self, encrypted_data_b64: str, aes_key: bytes) -> Optional[Dict]:
        """Decrypt location data using AES key - Flutter app actually uses XOR encryption"""
        try:
            encrypted_data = base64.b64decode(encrypted_data_b64)
            
            # Method 1: XOR decryption (current Flutter app format)
            # Despite the "AES-256-GCM" label, the Flutter app actually uses XOR
            try:
                decrypted_data = []
                for i in range(len(encrypted_data)):
                    decrypted_data.append(encrypted_data[i] ^ aes_key[i % len(aes_key)])
                
                decrypted_json = bytes(decrypted_data).decode('utf-8')
                return json.loads(decrypted_json)
            except Exception as e:
                print(f"⚠️ XOR decryption failed: {e}")
            
            # Method 2: Try AES-GCM (for future implementations)
            try:
                # AES-GCM format: nonce(12) + ciphertext + auth_tag(16)
                if len(encrypted_data) >= 28:  # minimum: 12 (nonce) + 16 (tag)
                    nonce = encrypted_data[:12]  # 12-byte nonce for GCM
                    auth_tag = encrypted_data[-16:]  # 16-byte authentication tag
                    ciphertext = encrypted_data[12:-16]  # everything in between
                    
                    cipher = Cipher(
                        algorithms.AES(aes_key),
                        modes.GCM(nonce, auth_tag),
                        backend=default_backend()
                    )
                    decryptor = cipher.decryptor()
                    decrypted_json = decryptor.update(ciphertext) + decryptor.finalize()
                    
                    return json.loads(decrypted_json.decode('utf-8'))
                    
            except Exception as e:
                print(f"⚠️ AES-GCM decryption failed: {e}")
            
            # Method 3: Try AES-CBC with IV (legacy format)
            if len(encrypted_data) > 16:
                try:
                    iv = encrypted_data[:16]
                    encrypted_content = encrypted_data[16:]
                    
                    cipher = Cipher(
                        algorithms.AES(aes_key),
                        modes.CBC(iv),
                        backend=default_backend()
                    )
                    decryptor = cipher.decryptor()
                    padded_data = decryptor.update(encrypted_content) + decryptor.finalize()
                    
                    # Remove PKCS7 padding
                    padding_length = padded_data[-1]
                    unpadded_data = padded_data[:-padding_length]
                    
                    decrypted_json = unpadded_data.decode('utf-8')
                    return json.loads(decrypted_json)
                    
                except Exception as e:
                    print(f"⚠️ AES-CBC decryption failed: {e}")
            
        except Exception as e:
            print(f"❌ Error decrypting location data: {e}")
            return None
    
    def process_csv_file(self, csv_path: str, output_dir: str = './decrypted_data') -> bool:
        """Process a single CSV file and decrypt all encrypted data based on survey_type"""
        
        if not os.path.exists(csv_path):
            print(f"❌ CSV file not found: {csv_path}")
            return False
        
        filename = os.path.basename(csv_path)
        print(f"\n🔍 Processing unified survey file: {filename}")
        
        try:
            # Read the CSV file
            import pandas as pd
            df = pd.read_csv(csv_path)
            
            # Skip Qualtrics header rows if present
            # Standard Qualtrics export has 3 header rows: column names, question labels, ImportId definitions
            if len(df) >= 3 and df.iloc[0].astype(str).str.contains('ImportId').any():
                print(f"📋 Detected Qualtrics header format, skipping first 2 rows")
                df = df.iloc[2:]  # Skip first 2 rows (headers)
                df.reset_index(drop=True, inplace=True)
            
            # Additional cleanup: remove any remaining header-like rows
            # Filter out rows that don't have actual ResponseId values
            initial_len = len(df)
            df = df[df['ResponseId'].notna() & (df['ResponseId'] != '') & 
                   ~df['ResponseId'].str.contains('ImportId|Response ID', na=False)]
            df.reset_index(drop=True, inplace=True)
            
            if len(df) < initial_len:
                print(f"📋 Cleaned data: {initial_len - len(df)} header/invalid rows removed")
            
            print(f"📊 Processing {len(df)} valid survey responses")
            
            # Check if survey_type column exists
            if 'survey_type' not in df.columns:
                print(f"❌ No 'survey_type' column found in data. Available columns: {list(df.columns)}")
                return False
            
            # Group data by survey type
            survey_type_groups = df.groupby('survey_type')
            print(f"📊 Found {len(survey_type_groups)} survey types:")
            
            total_decrypted = 0
            total_location_points = 0
            
            for survey_type, group_df in survey_type_groups:
                print(f"\n📋 Processing survey type: {survey_type} ({len(group_df)} responses)")
                
                # Find matching configuration  
                survey_config = self.survey_type_configs.get(survey_type)
                if not survey_config:
                    print(f"⚠️ Unknown survey type: {survey_type}, using default configuration")
                    survey_config = {
                        'encrypted_data_column': 'encrypted_data',
                        'location_column': 'encrypted_data' if survey_type == 'biweekly' else None,
                        'output_prefix': f'{survey_type}_decrypted'
                    }
                
                # Process this survey type
                decrypted_count, location_count = self._process_survey_group(
                    group_df, survey_config, output_dir
                )
                
                total_decrypted += decrypted_count
                total_location_points += location_count
            
            # Update results
            self.results['processed_files'].append(filename)
            self.results['decrypted_responses'] += total_decrypted
            self.results['location_points'] += total_location_points
            
            print(f"\n✅ Successfully processed {filename}")
            print(f"   Total decrypted responses: {total_decrypted}")
            print(f"   Total location points: {total_location_points}")
            
            return True
            
        except Exception as e:
            print(f"❌ Error processing {filename}: {e}")
            self.results['errors'].append(f"Processing error in {filename}: {e}")
            return False
    def _process_survey_group(self, group_df, survey_config: Dict, output_dir: str) -> Tuple[int, int]:
        """Process a group of responses for a specific survey type"""
        
        # Create output directory
        os.makedirs(output_dir, exist_ok=True)
        
        # Prepare output files
        output_responses = os.path.join(output_dir, f"{survey_config['output_prefix']}_responses.csv")
        output_locations = os.path.join(output_dir, f"{survey_config['output_prefix']}_locations.csv")
        
        decrypted_responses = []
        location_data_points = []
        
        try:
            # Process each response in the group
            for idx, (_, row) in enumerate(group_df.iterrows()):
                response_data = row.to_dict()
                response_id = response_data.get('ResponseId', f'row_{idx}')
                
                # Create decrypted response record (copy original response)
                decrypted_response = response_data.copy()
                
                # Process encrypted data if present
                encrypted_data_column = survey_config.get('encrypted_data_column', 'encrypted_data')
                encrypted_data = response_data.get(encrypted_data_column)
                if isinstance(encrypted_data, str) and encrypted_data:
                    decrypted_response.setdefault(
                        'encrypted_data_hash', hashlib.sha256(encrypted_data.encode('utf-8')).hexdigest()
                    )

                if encrypted_data_column in response_data and response_data[encrypted_data_column]:
                    encrypted_data = response_data[encrypted_data_column]
                    
                    # Skip non-encrypted data (like mock data strings)
                    if (isinstance(encrypted_data, str) and 
                        (encrypted_data.startswith('{"encryptedData"') or 
                         encrypted_data.startswith('eyJ'))):  # Base64 encoded JSON
                        
                        print(f"🔍 Processing encrypted data for {response_id}")
                        
                        # Try to decrypt the data
                        try:
                            # Handle different encryption formats
                            if encrypted_data.startswith('{"encryptedData"'):
                                # Standard JSON format
                                encrypted_package = json.loads(encrypted_data)
                            elif encrypted_data.startswith('eyJ'):
                                # Base64 encoded JSON format
                                import base64
                                decoded_json = base64.b64decode(encrypted_data).decode('utf-8')
                                encrypted_package = json.loads(decoded_json)
                            else:
                                continue
                            
                            # Extract encryption components
                            encrypted_data_b64 = encrypted_package.get('encryptedData')
                            encrypted_key_b64 = encrypted_package.get('encryptedKey')
                            
                            if encrypted_data_b64 and encrypted_key_b64:
                                # Decrypt AES key
                                aes_key = self.decrypt_aes_key(encrypted_key_b64)
                                if aes_key:
                                    # Decrypt the actual data
                                    decrypted_data = self.decrypt_location_data(encrypted_data_b64, aes_key)
                                    if decrypted_data:
                                        print(f"✅ Successfully decrypted data for {response_id}")
                                        
                                        # Replace encrypted data with decrypted content summary
                                        decrypted_response[encrypted_data_column] = f"DECRYPTED: {len(str(decrypted_data))} chars"
                                        
                                        # Extract location data if present and if this survey type should have it
                                        if (survey_config.get('location_column') == encrypted_data_column and 
                                            'locationData' in decrypted_data):
                                            
                                            locations = self._extract_location_points(decrypted_data, response_id)
                                            if locations:
                                                location_data_points.extend(locations)
                                                print(f"📍 Extracted {len(locations)} location points for {response_id}")
                                        
                                        # Store additional decrypted fields
                                        for key, value in decrypted_data.items():
                                            if key != 'locationData':  # Don't duplicate location data
                                                decrypted_response[f'decrypted_{key}'] = value
                                    else:
                                        decrypted_response[encrypted_data_column] = "DECRYPTION_FAILED"
                                        print(f"❌ Failed to decrypt data content for {response_id}")
                                else:
                                    decrypted_response[encrypted_data_column] = "KEY_DECRYPTION_FAILED"
                                    print(f"❌ Failed to decrypt AES key for {response_id}")
                            
                        except Exception as e:
                            print(f"❌ Error processing encrypted data for {response_id}: {e}")
                            decrypted_response[encrypted_data_column] = f"PROCESSING_ERROR: {str(e)[:100]}"
                    else:
                        # Non-encrypted data (like mock data) - just mark it
                        if isinstance(encrypted_data, str) and len(encrypted_data) > 0:
                            decrypted_response[encrypted_data_column] = f"NON_ENCRYPTED: {encrypted_data[:50]}..."
                
                decrypted_responses.append(decrypted_response)
            
            # Save decrypted responses
            if decrypted_responses:
                import pandas as pd
                output_df = pd.DataFrame(decrypted_responses)
                output_df.to_csv(output_responses, index=False)
                print(f"📁 Saved {len(decrypted_responses)} decrypted responses to: {output_responses}")
            
            # Save location data if any was extracted
            if location_data_points:
                import pandas as pd
                location_df = pd.DataFrame(location_data_points)
                location_df.to_csv(output_locations, index=False)
                print(f"📍 Saved {len(location_data_points)} location points to: {output_locations}")
            
            return len(decrypted_responses), len(location_data_points)
            
        except Exception as e:
            print(f"❌ Error processing survey group {survey_config.get('output_prefix', 'unknown')}: {e}")
            self.results['errors'].append(f"Processing error in {survey_config.get('output_prefix', 'unknown')}: {e}")
            return 0, 0
    
    def _extract_location_points(self, decrypted_data: Dict, response_id: str) -> List[Dict]:
        """Extract location points from decrypted data"""
        locations = []
        try:
            if 'locationData' in decrypted_data and isinstance(decrypted_data['locationData'], list):
                for i, loc in enumerate(decrypted_data['locationData']):
                    if isinstance(loc, dict):
                        locations.append({
                            'response_id': response_id,
                            'point_sequence': i + 1,
                            'timestamp': loc.get('timestamp', ''),
                            'latitude': loc.get('latitude', ''),
                            'longitude': loc.get('longitude', ''),
                            'accuracy': loc.get('accuracy', ''),
                            'altitude': loc.get('altitude', ''),
                            'speed': loc.get('speed', ''),
                            'heading': loc.get('heading', ''),
                            'provider': loc.get('provider', ''),
                            'battery_level': loc.get('batteryLevel', ''),
                            'is_mock': loc.get('isMock', '')
                        })
        except Exception as e:
            print(f"❌ Error extracting location points for {response_id}: {e}")
        
        return locations
    
    def _decrypt_response_location(self, encrypted_location: str, response_id: str) -> List[Dict]:
        """Decrypt location data for a single response"""
        try:
            # Handle different encryption formats
            if encrypted_location.startswith('{"encryptedData"'):
                # Standard JSON format
                encrypted_package = json.loads(encrypted_location)
                encrypted_data_b64 = encrypted_package['encryptedData']
                encrypted_key_b64 = encrypted_package['encryptedKey']
            elif encrypted_location.startswith('eyJ'):
                # Base64 encoded JSON format (like in the example)
                decoded_json = base64.b64decode(encrypted_location).decode('utf-8')
                encrypted_package = json.loads(decoded_json)
                encrypted_data_b64 = encrypted_package['encryptedData']
                encrypted_key_b64 = encrypted_package['encryptedKey']
            else:
                print(f"⚠️ Unknown encryption format for {response_id}: {encrypted_location[:50]}...")
                return []
            
            # Decrypt AES key
            aes_key = self.decrypt_aes_key(encrypted_key_b64)
            if not aes_key:
                return []
            
            # Decrypt location data
            location_data = self.decrypt_location_data(encrypted_data_b64, aes_key)
            if not location_data:
                return []
            
            # Extract location points
            locations = []
            if 'locationData' in location_data:
                for loc in location_data['locationData']:
                    locations.append({
                        'response_id': response_id,
                        'timestamp': loc.get('timestamp', ''),
                        'latitude': loc.get('latitude', ''),
                        'longitude': loc.get('longitude', ''),
                        'accuracy': loc.get('accuracy', ''),
                        'altitude': loc.get('altitude', ''),
                        'speed': loc.get('speed', ''),
                        'heading': loc.get('heading', '')
                    })
            
            return locations
            
        except Exception as e:
            print(f"❌ Error decrypting location for {response_id}: {e}")
            return []
    
    def _save_csv(self, filepath: str, data: List[Dict], headers: List[str]) -> None:
        """Save data to CSV file"""
        with open(filepath, 'w', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=headers)
            writer.writeheader()
            for row in data:
                # Only write columns that exist in headers
                filtered_row = {k: v for k, v in row.items() if k in headers}
                writer.writerow(filtered_row)
    
    def run_download_first(self, download_args: List[str]) -> bool:
        """Run the download script first"""
        try:
            download_cmd = ['python3', 'download_qualtrics_data.py'] + download_args
            print(f"🔄 Running download command: {' '.join(download_cmd)}")
            
            result = subprocess.run(download_cmd, capture_output=True, text=True)
            
            if result.returncode == 0:
                print("✅ Download completed successfully")
                print(result.stdout)
                return True
            else:
                print(f"❌ Download failed: {result.stderr}")
                self.results['errors'].append(f"Download error: {result.stderr}")
                return False
                
        except Exception as e:
            print(f"❌ Error running download: {e}")
            self.results['errors'].append(f"Download execution error: {e}")
            return False
    
    def process_directory(self, input_dir: str, output_dir: str = './decrypted_data') -> bool:
        """Process all CSV files in a directory"""
        
        if not os.path.exists(input_dir):
            print(f"❌ Input directory not found: {input_dir}")
            return False
        
        # Find all CSV files
        csv_files = []
        for filename in os.listdir(input_dir):
            if filename.endswith('.csv'):
                csv_files.append(os.path.join(input_dir, filename))
        
        if not csv_files:
            print(f"❌ No CSV files found in {input_dir}")
            return False
        
        print(f"📂 Found {len(csv_files)} CSV files to process")
        
        success_count = 0
        for csv_file in csv_files:
            print(f"\\n{'='*60}")
            if self.process_csv_file(csv_file, output_dir):
                success_count += 1
        
        print(f"\\n🎉 Processed {success_count}/{len(csv_files)} files successfully")
        return success_count > 0
    
    def print_summary(self) -> None:
        """Print processing summary"""
        print(f"\\n{'='*60}")
        print("📊 DECRYPTION PIPELINE SUMMARY")
        print(f"{'='*60}")
        print(f"Files processed: {len(self.results['processed_files'])}")
        print(f"Responses decrypted: {self.results['decrypted_responses']}")
        print(f"Location points extracted: {self.results['location_points']}")
        print(f"Errors encountered: {len(self.results['errors'])}")
        
        if self.results['processed_files']:
            print(f"\\n📁 Processed files:")
            for filename in self.results['processed_files']:
                print(f"   ✅ {filename}")
        
        if self.results['errors']:
            print(f"\\n❌ Errors:")
            for error in self.results['errors']:
                print(f"   • {error}")


def main():
    parser = argparse.ArgumentParser(
        description='Automated decryption pipeline for Qualtrics survey data',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --input ./data                    Process all CSV files in ./data
  %(prog)s --file survey_responses.csv       Process specific file
  %(prog)s --download-first --days 7         Download last 7 days then decrypt
  %(prog)s --download-first --all            Download all data then decrypt
        """
    )
    
    # Input options
    parser.add_argument('--input', default='./data',
                       help='Input directory containing CSV files (default: ./data)')
    
    parser.add_argument('--file', 
                       help='Process specific CSV file')
    
    parser.add_argument('--output', default='./decrypted_data',
                       help='Output directory for decrypted data (default: ./decrypted_data)')
    
    # Download integration
    parser.add_argument('--download-first', action='store_true',
                       help='Run download script first, then decrypt')
    
    parser.add_argument('--days', type=int,
                       help='Download data from last N days (use with --download-first)')
    
    parser.add_argument('--all', action='store_true',
                       help='Download all survey data (use with --download-first)')
    
    parser.add_argument('--survey', choices=['initial', 'biweekly', 'consent'],
                       help='Download specific survey (use with --download-first)')
    
    # Security options
    parser.add_argument('--private-key', default='../pipeline_toolkit/secrets/private_key.pem',
                       help='Path to RSA private key file (default: ../pipeline_toolkit/secrets/private_key.pem)')
    
    parser.add_argument('--password', 
                       help='Private key password (will prompt if not provided)')
    
    args = parser.parse_args()
    
    # Create pipeline
    pipeline = AutomatedDecryptionPipeline(args.private_key)
    
    # Get private key password if needed
    password = args.password or os.environ.get("PRIVATE_KEY_PASSWORD")
    if not password and os.path.exists(args.private_key):
        # Check if key is encrypted by trying to load without password
        try:
            with open(args.private_key, 'rb') as f:
                key_data = f.read()
                if b'ENCRYPTED' in key_data:
                    import getpass
                    password = getpass.getpass("Enter private key password: ")
        except:
            pass
    
    # Load private key
    if not pipeline.load_private_key(password):
        print("❌ Failed to load private key. Exiting.")
        return 1
    
    # Run download first if requested
    if args.download_first:
        download_args = []
        
        if args.all:
            download_args.append('--all')
        elif args.survey:
            download_args.extend(['--survey', args.survey])
        else:
            print("❌ Must specify --all or --survey when using --download-first")
            return 1
        
        if args.days:
            download_args.extend(['--days', str(args.days)])
        
        download_args.extend(['--output', args.input])
        
        if not pipeline.run_download_first(download_args):
            print("❌ Download failed. Exiting.")
            return 1
    
    # Process data
    success = False
    if args.file:
        success = pipeline.process_csv_file(args.file, args.output)
    else:
        success = pipeline.process_directory(args.input, args.output)
    
    # Print summary
    pipeline.print_summary()
    
    return 0 if success else 1


if __name__ == '__main__':
    sys.exit(main())