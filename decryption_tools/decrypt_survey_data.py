#!/usr/bin/env python3
"""
Decrypt survey data using the private key
"""

import base64
import json
import csv
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa, padding
from cryptography.hazmat.backends import default_backend
import getpass
from pathlib import Path

def load_private_key(private_key_path, passphrase=None):
    """Load the private key from file"""
    with open(private_key_path, 'rb') as key_file:
        private_key = serialization.load_pem_private_key(
            key_file.read(),
            password=passphrase,
            backend=default_backend()
        )
    return private_key

def decrypt_data(encrypted_data_b64, private_key_path, passphrase):
    """
    Decrypt base64-encoded encrypted data using RSA private key.
    Handles both direct RSA encryption and hybrid AES/RSA encryption.
    """
    try:
        # Try to parse as JSON first (hybrid encryption format)
        try:
            encrypted_package = json.loads(base64.b64decode(encrypted_data_b64).decode('utf-8'))
            if 'encryptedData' in encrypted_package and 'encryptedKey' in encrypted_package:
                print("Detected hybrid AES/RSA encryption format")
                return decrypt_hybrid_format(encrypted_package, private_key_path, passphrase)
        except:
            # Not JSON or not hybrid format, continue with direct RSA
            pass
        
        # Direct RSA encryption format
        print("Using direct RSA decryption")
        encrypted_data = base64.b64decode(encrypted_data_b64)
        print(f"Decoded encrypted data length: {len(encrypted_data)} bytes")
        
        # Load private key
        with open(private_key_path, 'rb') as key_file:
            private_key = serialization.load_pem_private_key(
                key_file.read(),
                password=passphrase.encode() if passphrase else None,
                backend=default_backend()
            )
        
        print(f"Private key size: {private_key.key_size // 8} bytes")
        
        # Check if data size exceeds RSA key capacity
        max_decrypt_size = private_key.key_size // 8
        if len(encrypted_data) > max_decrypt_size:
            print(f"❌ Encrypted data ({len(encrypted_data)} bytes) exceeds RSA key capacity ({max_decrypt_size} bytes)")
            print("This suggests the data was encrypted using a different method or key")
            return None
        
        # Decrypt using PKCS1v15 padding (matching Flutter app)
        decrypted_bytes = private_key.decrypt(
            encrypted_data,
            padding.PKCS1v15()
        )
        
        # Convert to string and parse JSON
        decrypted_str = decrypted_bytes.decode('utf-8')
        survey_data = json.loads(decrypted_str)
        
        return survey_data
        
    except Exception as e:
        print(f"Decryption failed: {e}")
        import traceback
        traceback.print_exc()
        return None

def decrypt_hybrid_format(encrypted_package, private_key_path, passphrase):
    """
    Decrypt hybrid AES/RSA encrypted data (like in archive scripts)
    """
    try:
        # Extract components and fix base64 padding
        encrypted_data_b64 = fix_base64_padding(encrypted_package['encryptedData'])
        encrypted_key_b64 = fix_base64_padding(encrypted_package['encryptedKey'])
        
        encrypted_data = base64.b64decode(encrypted_data_b64)
        encrypted_key = base64.b64decode(encrypted_key_b64)
        
        print(f"Hybrid format - Data: {len(encrypted_data)} bytes, Key: {len(encrypted_key)} bytes")
        
        # Load the private key
        with open(private_key_path, 'rb') as key_file:
            private_key = serialization.load_pem_private_key(
                key_file.read(),
                password=passphrase.encode() if passphrase else None,
                backend=default_backend()
            )
        
        # Decrypt the AES key using RSA PKCS1v15 padding
        aes_key_data = private_key.decrypt(
            encrypted_key,
            padding.PKCS1v15()
        )
        
        # Check if the decrypted data is base64-encoded AES key
        try:
            # Try to decode as base64 first (Flutter app base64-encodes the AES key)
            aes_key = base64.b64decode(aes_key_data.decode('utf-8'))
            print(f"Successfully decrypted base64-encoded AES key: {len(aes_key)} bytes")
        except:
            # If base64 decoding fails, use raw bytes (archive format)
            aes_key = aes_key_data
            print(f"Successfully decrypted raw AES key: {len(aes_key)} bytes")
        
        # Decrypt data using XOR (as used in archive scripts)
        decrypted_data = []
        for i in range(len(encrypted_data)):
            decrypted_data.append(encrypted_data[i] ^ aes_key[i % len(aes_key)])
        
        # Convert back to string and parse JSON
        decrypted_str = bytes(decrypted_data).decode('utf-8')
        survey_data = json.loads(decrypted_str)
        
        return survey_data
        
    except Exception as e:
        print(f"Hybrid decryption failed: {e}")
        import traceback
        traceback.print_exc()
        return None

def fix_base64_padding(data):
    """Fix base64 padding if needed"""
    missing_padding = len(data) % 4
    if missing_padding:
        data += '=' * (4 - missing_padding)
    return data

def load_test_surveys(csv_file: str | None = None):
    """Load encrypted survey data from CSV file.

    Falls back to the most recent decrypted pipeline output or
    hard-coded fixtures if nothing is available.
    """
    surveys: list[dict[str, str]] = []

    default_paths = [
        Path('../data/decrypted/biweekly_decrypted_responses.csv'),
        Path('../data/decrypted/initial_decrypted_responses.csv'),
        Path('../data/decrypted/consent_decrypted_responses.csv')
    ]

    chosen_path: Path | None = None

    if csv_file:
        candidate = Path(csv_file)
        if candidate.exists():
            chosen_path = candidate
        else:
            print(f"❌ CSV file '{csv_file}' not found")
    else:
        for candidate in default_paths:
            if candidate.exists():
                chosen_path = candidate
                break

    if chosen_path is None:
        print("ℹ️ No CSV file found. Using hardcoded test data instead...")
        return get_hardcoded_test_data()

    try:
        with chosen_path.open('r') as f:
            reader = csv.reader(f)
            headers = next(reader)  # Skip header row
            
            for row in reader:
                if len(row) >= 2:  # Ensure we have at least timestamp and encrypted_data
                    timestamp = row[0]
                    encrypted_data = row[1]
                    
                    # Determine survey type from CSV filename or content
                    survey_type = 'unknown'
                    if 'biweekly' in chosen_path.name.lower():
                        survey_type = 'biweekly'
                    elif 'initial' in chosen_path.name.lower():
                        survey_type = 'initial'
                    elif 'consent' in chosen_path.name.lower():
                        survey_type = 'consent'
                    
                    surveys.append({
                        'timestamp': timestamp,
                        'data': encrypted_data,
                        'type': survey_type
                    })
    
    except FileNotFoundError:
        print(f"❌ CSV file '{chosen_path}' not found")
        print("Using hardcoded test data instead...")
        return get_hardcoded_test_data()
    except Exception as e:
        print(f"❌ Error loading test surveys: {e}")
        return []
    
    print(f"📊 Loaded {len(surveys)} encrypted surveys from {chosen_path}")
    return surveys

def get_hardcoded_test_data():
    """Fallback test data if CSV file not available"""
    return [
        {
            "data": "Pxd6ECC/rfPQrF7Wdy2c5/Pzpqug37NHzxG6zGorchjB20VTri8zhYQTH0hJyMFv8LjpP8dWzUHuwBrKArGSMCJD0JRHmRGNwxJ7/VQM9LCXlOwHfSVPmE5W/OE2lVk/lk6evgYQfy6sjR4stRAntUof8DQcmIoFMViHLHEP2bvaH7cdJ4as9kjiWF7GD1KW20ry0XkmNGhKnb2wYZxghBMvcWtbZonS+Buw0FUS94+F9RBNalLbSPR/qBipshnHVvoXyz10v155NLCiG3U6adp2pvcniAiFcqmrvtKtRheE2/myhariLHXmljIyzccJpkAjXIWdD0R9hEcAVT+OuxKfKXaeal1SmrmJI6sLteoBFXlybtcAdeY/52K5AGQbj9zPM/ZzNJ9qhhV/eduxiU/Ap6igMZ4mzEpYJVdfvr66+uIeLA9NFXr3cgOz+xyVU2znTLetYa4oqHFZaxXBuuRZ9O8NEaabgQIXM5GHhGRg29yFGvHYoKd9OrPRUFlTSF8BsO2oAN+dyqTUr4hwPZ0FDeYSVCdlg6S3kShb+tceEzfW5O7ZY//2WtOefBdNE/0WganqrMN1B6bQWZ25ckKzdZM0fzoANrAuje7qt+4TshIM9fN9g1NuLkNNNKYDG5PFUb7TQgR2Ip2ku5D9vHOAqSLwAr2vejHIm5PAGOZ+wxRjNHgiYYGZebPQawnJAFnlATLQshFH9YtZnQrneRDggHl7JaLbpElWYqftj4lMb8gaukm0mCOE/2ohQohG4RKCFQOmZJz/CNrJRAKr4ynQN2oBDFKdt09zIs3l/EZoxMqh52VBC5X3m46HWKygpYwYvtog6rWCVUJBEosecVRIzuuAQQToGyOgMtjWvJrE1gL1DV/PrK4SCcG2XREtpGxJl1CFe54mZ2b+uAngsCRSOSjCXjWDfaQ2F6q+RJADZIWN/tZfKo8RJMwgwuu7gdFM+0W5nRTiW0MVvHEML/srczSGYroMk8Jf80hegIYf1fBq1gI3w/EJbgPnUXWVCCLSPFh5XyjayUxSJPjyRueSImnh307fp362kK5P+OiqDLr8MYL8vxsAtBiRzCRYga9TpLws0fXxkT08Ew2VjqDbDkh1GRjncnFKz5Hpr6FkyT8+2HyS8OKjTcSHaoU17rAM1n0webSjRbjbeaqBaJFBj7Y1ZJqv9iMddDKON5vNw6HTyLalU2PjUGEuSZDMVElucTVNxiQUzf9c12+jFMd1QE+3CiTEXd+LGSEaTAmxkYaboiKxBtiGq40u8+4C8HQR5XNHFpKzxku/j3f3znLoINbxkoKdnJHjIE73gBItDwdo1W1sw4Y/7H5Nxt/RKNcLURtAL65hvDbT8Mg47g==",
            "type": "consent"
        },
        {
            "data": "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",
            "type": "biweekly"
        }
    ]

def main():
    import sys
    
    # Check if a CSV file was provided as argument
    csv_file = sys.argv[1] if len(sys.argv) > 1 else None
    
    # Load surveys from CSV file or use hardcoded data
    encrypted_surveys = load_test_surveys(csv_file)
    
    try:
        # Get passphrase from user
        passphrase = getpass.getpass("Enter private key passphrase: ")
        
        key_path = '../pipeline_toolkit/secrets/private_key.pem'
        # Load private key to verify it works
        private_key = load_private_key(key_path, passphrase.encode())
        print("✅ Private key loaded successfully")
        
        # Decrypt each survey
        for i, survey in enumerate(encrypted_surveys):
            print(f"\n{'='*50}")
            print(f"Decrypting {survey['type']} survey #{i+1}")
            print(f"{'='*50}")
            
            decrypted_data = decrypt_data(survey['data'], key_path, passphrase)
            
            if decrypted_data:
                print("✅ Decryption successful!")
                print(json.dumps(decrypted_data, indent=2))
            else:
                print("❌ Decryption failed")
                
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()