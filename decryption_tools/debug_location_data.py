#!/usr/bin/env python3
"""
Debug script to examine raw decrypted JSON for location data
"""

import json
import base64
import os
import sys
from pathlib import Path

# Import the decryption pipeline
from automated_decryption_pipeline import AutomatedDecryptionPipeline

def debug_latest_surveys():
    """Debug the latest survey responses to check for location data"""
    
    # Initialize decryption pipeline
    pipeline = AutomatedDecryptionPipeline()
    
    # Load private key
    password = input("Enter private key password: ")
    if not pipeline.load_private_key(password):
        print("Failed to load private key")
        return
    
    # Read the latest CSV data
    import pandas as pd
    csv_path = "../qualtrics_tools/data/wellbeing_mapper_responses.csv"
    df = pd.read_csv(csv_path)
    
    # Focus on the latest biweekly surveys
    latest_surveys = ['R_bPeXsPo9TlqIhi6', 'R_0VBX9jGiaWjqanI']
    
    for response_id in latest_surveys:
        print(f"\n=== Analyzing {response_id} ===")
        
        # Find the row
        row = df[df['ResponseId'] == response_id]
        if row.empty:
            print(f"Response {response_id} not found")
            continue
            
        encrypted_data = row['encrypted_data'].iloc[0]
        
        if not encrypted_data or pd.isna(encrypted_data):
            print(f"No encrypted data for {response_id}")
            continue
            
        if not (isinstance(encrypted_data, str) and 
                (encrypted_data.startswith('{"encryptedData"') or 
                 encrypted_data.startswith('eyJ'))):
            print(f"Data doesn't look encrypted: {encrypted_data[:50]}...")
            continue
            
        print(f"Encrypted data length: {len(encrypted_data)} chars")
        
        try:
            # Decrypt the data
            if encrypted_data.startswith('eyJ'):
                # Base64 encoded JSON format
                decoded_json = base64.b64decode(encrypted_data).decode('utf-8')
                encrypted_package = json.loads(decoded_json)
            else:
                # Standard JSON format
                encrypted_package = json.loads(encrypted_data)
            
            print(f"Encryption package keys: {list(encrypted_package.keys())}")
            
            # Extract encryption components
            encrypted_data_b64 = encrypted_package.get('encryptedData')
            encrypted_key_b64 = encrypted_package.get('encryptedKey')
            
            if not encrypted_data_b64 or not encrypted_key_b64:
                print("Missing encryptedData or encryptedKey")
                continue
                
            # Decrypt AES key
            aes_key = pipeline.decrypt_aes_key(encrypted_key_b64)
            if not aes_key:
                print("Failed to decrypt AES key")
                continue
                
            print(f"AES key length: {len(aes_key)} bytes")
            
            # Decrypt the actual data
            decrypted_data = pipeline.decrypt_location_data(encrypted_data_b64, aes_key)
            if not decrypted_data:
                print("Failed to decrypt data")
                continue
                
            print(f"Decrypted data keys: {list(decrypted_data.keys())}")
            
            # Check for location-related fields
            location_fields = [k for k in decrypted_data.keys() if 'location' in k.lower()]
            print(f"Location-related fields: {location_fields}")
            
            # Print location_data specifically
            if 'location_data' in decrypted_data:
                location_data = decrypted_data['location_data']
                print(f"location_data value: {location_data}")
                print(f"location_data type: {type(location_data)}")
                
                if location_data and isinstance(location_data, (list, dict)):
                    print(f"Location data structure: {json.dumps(location_data, indent=2)}")
                elif location_data:
                    print(f"Location data content: {location_data}")
                else:
                    print("Location data is empty/None")
            
            # Check for other location fields
            for field in ['encrypted_location_data', 'locationData']:
                if field in decrypted_data:
                    location_data = decrypted_data[field]
                    print(f"{field}: {location_data}")
                    
            # Print survey data structure
            if 'data' in decrypted_data and isinstance(decrypted_data['data'], dict):
                data_keys = list(decrypted_data['data'].keys())
                print(f"Data field keys: {data_keys}")
                
                # Check if encrypted_location_data is in the data sub-object
                if 'encrypted_location_data' in decrypted_data['data']:
                    eld = decrypted_data['data']['encrypted_location_data']
                    print(f"data.encrypted_location_data: {eld}")
            
            print(f"\n--- Key fields for {response_id} ---")
            print(f"Type: {decrypted_data.get('type')}")
            print(f"Participant: {decrypted_data.get('participant_uuid')}")
            print(f"Survey ID: {decrypted_data.get('survey_id')}")
            print(f"Timestamp: {decrypted_data.get('timestamp')}")
            print(f"Has location_data: {'location_data' in decrypted_data}")
            print(f"Location data empty: {not decrypted_data.get('location_data')}")
            
        except Exception as e:
            print(f"Error processing {response_id}: {e}")
            import traceback
            traceback.print_exc()

if __name__ == "__main__":
    debug_latest_surveys()