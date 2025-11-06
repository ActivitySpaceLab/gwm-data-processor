#!/usr/bin/env python3
"""
Qualtrics Data Download Script
==============================

This script automatically downloads survey response data from Qualtrics using their API.
It supports all three surveys used in the Gauteng Wellbeing Mapper project:
- Initial Demographics Survey
- Biweekly Wellbeing Survey  
- Consent Form Survey

Features:
- Bulk data export for analysis
- Automated CSV download and processing
- Configurable date ranges
- Progress tracking and retry logic
- Structured output files

Usage:
    python download_qualtrics_data.py [options]
    
Examples:
    # Download all data
    python download_qualtrics_data.py --all
    
    # Download specific survey data
    python download_qualtrics_data.py --survey initial
    
    # Download data from last 30 days
    python download_qualtrics_data.py --days 30
    
    # Download to custom directory
    python download_qualtrics_data.py --output /path/to/data/
"""

import argparse
import json
import os
import sys
import time
import zipfile
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import requests
import pandas as pd
from pathlib import Path


def _ensure_utf8_output() -> None:
    """Force stdout/stderr to UTF-8 so Windows consoles avoid charmap errors."""

    if os.name == "nt":
        try:
            import ctypes  # type: ignore

            ctypes.windll.kernel32.SetConsoleOutputCP(65001)
            ctypes.windll.kernel32.SetConsoleCP(65001)
        except Exception:
            pass

    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass


_ensure_utf8_output()

class QualtricsDataDownloader:
    """Download survey response data from Qualtrics API"""
    
    def __init__(self, api_token: str, base_url: str, survey_id: str):
        self.api_token = api_token
        self.base_url = base_url
        self.survey_id = survey_id
        self.session = requests.Session()
        self.session.headers.update({
            'X-API-TOKEN': api_token,
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        })

        # Single survey configuration - all survey types are stored in one Qualtrics survey
        # and differentiated by the 'survey_type' column in the data
        self.survey_name = 'Gauteng Wellbeing Mapper Survey'
        self.output_filename = 'wellbeing_mapper_responses.csv'
    
    def download_survey_responses(self, output_dir: str = './data', 
                                start_date: Optional[datetime] = None, 
                                end_date: Optional[datetime] = None) -> bool:
        """Download responses from the unified Gauteng Wellbeing Mapper survey"""
        
        print(f"\n📊 Downloading {self.survey_name} data...")
        print(f"Survey ID: {self.survey_id}")
        
        try:
            # Step 1: Create export request
            export_id = self._create_export_request(self.survey_id, start_date, end_date)
            if not export_id:
                return False
                
            # Step 2: Wait for export to complete
            download_url = self._wait_for_export_completion(self.survey_id, export_id)
            if not download_url:
                return False
                
            # Step 3: Download the file
            output_path = os.path.join(output_dir, self.output_filename)
            success = self._download_export_file(self.survey_id, download_url, output_path)
            
            if success:
                print(f"✅ {self.survey_name} data downloaded to: {output_path}")
                # Show basic stats including survey type breakdown
                self._show_data_stats(output_path)
                return True
            else:
                return False
                
        except Exception as e:
            print(f"❌ Error downloading {self.survey_name}: {e}")
            return False
    
    def _create_export_request(self, survey_id: str, start_date: Optional[datetime], 
                              end_date: Optional[datetime]) -> Optional[str]:
        """Create a data export request in Qualtrics"""
        
        # Build request payload
        payload = {
            'format': 'csv',
            'compress': False,
            'useLabels': True,
            'seenUnansweredRecode': -999,
            'multiselectSeenUnansweredRecode': -999,
            'includeDisplayOrder': True
        }
        
        # Add date filters if specified
        if start_date or end_date:
            payload['dateRange'] = {}
            if start_date:
                payload['dateRange']['startDate'] = start_date.isoformat() + 'Z'
            if end_date:
                payload['dateRange']['endDate'] = end_date.isoformat() + 'Z'
        
        try:
            response = self.session.post(
                f"{self.base_url}/surveys/{survey_id}/export-responses",
                json=payload
            )
            
            if response.status_code == 200:
                result = response.json()
                export_id = result['result']['progressId'] 
                print(f"🔄 Export request created: {export_id}")
                return export_id
            else:
                print(f"❌ Failed to create export request: {response.status_code}")
                print(f"Response: {response.text}")
                return None
                
        except Exception as e:
            print(f"❌ Error creating export request: {e}")
            return None
    
    def _wait_for_export_completion(self, survey_id: str, export_id: str, 
                                   max_wait_time: int = 300) -> Optional[str]:
        """Wait for export to complete and return download URL"""
        
        print("⏳ Waiting for export to complete...")
        start_time = time.time()
        
        while time.time() - start_time < max_wait_time:
            try:
                response = self.session.get(
                    f"{self.base_url}/surveys/{survey_id}/export-responses/{export_id}"
                )
                
                if response.status_code == 200:
                    result = response.json()
                    status = result['result']['status']
                    
                    if status == 'complete':
                        download_url = result['result']['fileId']
                        print(f"✅ Export completed! File ID: {download_url}")
                        return download_url
                    elif status == 'failed':
                        print(f"❌ Export failed: {result.get('result', {}).get('error', 'Unknown error')}")
                        return None
                    else:
                        print(f"🔄 Export status: {status}")
                        
                else:
                    print(f"❌ Error checking export status: {response.status_code}")
                    return None
                    
            except Exception as e:
                print(f"❌ Error checking export status: {e}")
                return None
            
            time.sleep(5)  # Wait 5 seconds before checking again
        
        print(f"⏰ Export timeout after {max_wait_time} seconds")
        return None
    
    def _download_export_file(self, survey_id: str, file_id: str, output_path: str) -> bool:
        """Download the exported file"""
        
        # Create output directory if it doesn't exist
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        try:
            response = self.session.get(
                f"{self.base_url}/surveys/{survey_id}/export-responses/{file_id}/file",
                stream=True
            )
            
            if response.status_code == 200:
                # Check if it's a zip file
                content_type = response.headers.get('content-type', '')
                if 'zip' in content_type:
                    # Handle zip file
                    zip_path = output_path.replace('.csv', '.zip')
                    with open(zip_path, 'wb') as f:
                        for chunk in response.iter_content(chunk_size=8192):
                            f.write(chunk)
                    
                    # Extract CSV from zip
                    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                        csv_files = [f for f in zip_ref.namelist() if f.endswith('.csv')]
                        if csv_files:
                            zip_ref.extract(csv_files[0], os.path.dirname(output_path))
                            # Rename to expected filename
                            extracted_path = os.path.join(os.path.dirname(output_path), csv_files[0])
                            os.rename(extracted_path, output_path)
                            os.remove(zip_path)  # Clean up zip file
                        else:
                            print("❌ No CSV file found in zip archive")
                            return False
                else:
                    # Direct CSV file
                    with open(output_path, 'wb') as f:
                        for chunk in response.iter_content(chunk_size=8192):
                            f.write(chunk)
                
                print(f"📁 File downloaded: {output_path}")
                return True
            else:
                print(f"❌ Failed to download file: {response.status_code}")
                print(f"Response: {response.text}")
                return False
                
        except Exception as e:
            print(f"❌ Error downloading file: {e}")
            return False
    
    def _show_data_stats(self, csv_path: str) -> None:
        """Show basic statistics about the downloaded data"""
        try:
            df = pd.read_csv(csv_path)
            
            # Skip Qualtrics header rows if present
            if df.iloc[0].astype(str).str.contains('ImportId').any():
                df = df.iloc[2:]  # Skip first 2 rows (headers)
                df.reset_index(drop=True, inplace=True)
            
            print(f"📈 Data Statistics:")
            print(f"   Total responses: {len(df)}")
            print(f"   Columns: {len(df.columns)}")
            
            # Show survey type breakdown if survey_type column exists
            if 'survey_type' in df.columns:
                survey_type_counts = df['survey_type'].value_counts()
                print(f"   Survey types breakdown:")
                for survey_type, count in survey_type_counts.items():
                    print(f"     {survey_type}: {count} responses")
            
            # Show date range if timestamp column exists
            date_columns = [col for col in df.columns if 'recorded' in col.lower() or 'date' in col.lower() or 'timestamp' in col.lower()]
            if date_columns:
                date_col = date_columns[0]
                df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
                valid_dates = df[date_col].dropna()
                if len(valid_dates) > 0:
                    print(f"   Date range: {valid_dates.min()} to {valid_dates.max()}")
            
        except Exception as e:
            print(f"❌ Error reading downloaded file for stats: {e}")
    
    def download_all_data(self, output_dir: str = './data', 
                         start_date: Optional[datetime] = None,
                         end_date: Optional[datetime] = None) -> bool:
        """Download all survey data from the unified survey"""
        
        print(f"\n🚀 Starting download of all survey data...")
        print(f"Output directory: {output_dir}")
        if start_date:
            print(f"Start date: {start_date}")
        if end_date:
            print(f"End date: {end_date}")
        
        success = self.download_survey_responses(output_dir, start_date, end_date)
        
        if success:
            print(f"\n✅ All survey data downloaded successfully!")
        else:
            print(f"\n❌ Download failed!")
            
        return success
    
    def show_survey_info(self) -> None:
        """Show information about the unified survey"""
        print("\n📋 Survey Information:")
        print(f"   Survey: {self.survey_name}")
        print(f"   Survey ID: {self.survey_id}")
        print(f"   Data types: All survey types (initial, biweekly, consent)")
        print(f"   Differentiated by: 'survey_type' column in responses")


def main():
    parser = argparse.ArgumentParser(
        description='Download survey response data from Qualtrics',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --all                    Download all survey data
  %(prog)s --days 30                Download data from last 30 days
  %(prog)s --start 2024-01-01       Download data from specific start date
  %(prog)s --end 2024-12-31         Download data up to specific end date
  %(prog)s --output ./my_data/      Save to custom directory

Environment Variables:
  QUALTRICS_API_TOKEN              Your Qualtrics API token
    QUALTRICS_BASE_URL               Qualtrics API base URL
    QUALTRICS_SURVEY_ID              Qualtrics survey ID for the unified mapper survey
        """
    )
    
    # API configuration
    parser.add_argument('--api-token', 
                       default=os.environ.get('QUALTRICS_API_TOKEN', ''),
                       help='Qualtrics API token (default: from QUALTRICS_API_TOKEN env var)')
    
    parser.add_argument('--base-url',
                       default=os.environ.get('QUALTRICS_BASE_URL', ''),
                       help='Qualtrics API base URL (default: from QUALTRICS_BASE_URL env var)')
    
    parser.add_argument('--survey-id',
                       default=os.environ.get('QUALTRICS_SURVEY_ID', ''),
                       help='Qualtrics survey ID (default: from QUALTRICS_SURVEY_ID env var)')
    
    # Survey selection
    parser.add_argument('--all', action='store_true',
                       help='Download all survey data (default action)')
    
    parser.add_argument('--info', action='store_true',
                       help='Show survey information and exit')
    
    # Date filtering
    parser.add_argument('--days', type=int,
                       help='Download data from last N days')
    
    parser.add_argument('--start', type=str,
                       help='Start date (YYYY-MM-DD)')
    
    parser.add_argument('--end', type=str,
                       help='End date (YYYY-MM-DD)')
    
    # Output configuration
    parser.add_argument('--output', default='./data',
                       help='Output directory (default: ./data)')
    
    args = parser.parse_args()
    
    # Check for API token
    if not args.api_token:
        print("❌ Error: No Qualtrics API token provided")
        print("Set the QUALTRICS_API_TOKEN environment variable or use --api-token")
        print("\nTo get your API token:")
        print("1. Log into Qualtrics")
        print("2. Go to Account Settings > Qualtrics IDs")
        print("3. Generate a new API token")
        print("4. Export QUALTRICS_API_TOKEN='your_token_here'")
        return
    
    if not args.base_url:
        print("❌ Error: No Qualtrics base URL provided")
        print("Set QUALTRICS_BASE_URL in your environment or pass --base-url")
        return

    if not args.survey_id:
        print("❌ Error: No Qualtrics survey ID provided")
        print("Set QUALTRICS_SURVEY_ID in your environment or pass --survey-id")
        return

    # Create downloader
    downloader = QualtricsDataDownloader(args.api_token, args.base_url, args.survey_id)
    
    # Handle info command
    if args.info:
        downloader.show_survey_info()
        return
    
    # Parse date arguments
    start_date = None
    end_date = None
    
    if args.days:
        end_date = datetime.now()
        start_date = end_date - timedelta(days=args.days)
    
    if args.start:
        try:
            start_date = datetime.fromisoformat(args.start)
        except ValueError:
            print(f"❌ Invalid start date format: {args.start} (use YYYY-MM-DD)")
            return
    
    if args.end:
        try:
            end_date = datetime.fromisoformat(args.end)
        except ValueError:
            print(f"❌ Invalid end date format: {args.end} (use YYYY-MM-DD)")
            return
    
    # Execute download (default to --all if no specific action)
    success = downloader.download_all_data(args.output, start_date, end_date)
    if success:
        print(f"\n🎉 Download completed successfully!")
    else:
        print(f"\n❌ Download failed!")


if __name__ == '__main__':
    # Check dependencies
    try:
        import pandas as pd
        import requests
    except ImportError as e:
        print(f"❌ Missing required dependency: {e}")
        print("Install with: pip install pandas requests")
        exit(1)
    
    main()