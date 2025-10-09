#!/usr/bin/env python3
"""
Test hybrid AES/RSA decryption with the archive format
"""

import json
import base64
from decrypt_survey_data import decrypt_hybrid_format

# Test data from archive/decrypt_test_data.py
test_hybrid_data = {
    "encryptedData": "GEYJCQQJHQMEAikPGxFTSCgPVxoWDBAODhgYXEW0saxUXFRRU1BdRkkAAgAIGQUHFxFXTFpPSlRCSkVJT7G3rkEQDAsCGx0LBhxPVE1CQUBGWUVOWklJLkpKR09GurW7TV1TVD1KRUgKDw4bHRESC1FORlhGQVVYGhAJFwv15edBXkhVSVtFSBgcCAsLUktCXUVCWlUZGg4SChQKBqK7oBQFCQ0OBg5IFkAWTAMRBRsHARETVUJNSlVLRU5LtrSuQQgKCAABHR8PCU9UQkdCXEpCTU9PQE9WWQgUExrz9eMOFEdcRVpZWF5BXVZCQUEmQkJPRE9CTUxVTEpJJaKtoAIHBhMVCQoTSVZbQFxEXVASGAEfAw0dH1lGUE1Rs62gEBQAAwNKU1pFXVlCTRESBhoCHAIOWkNYEhIiCBro6OEPAUcbSxNLBgoYBBoaFBRQSUBFWEBASElMTU9SXezu7AQNERMDDUtQRlteQFZGSEJDQkFaVQwQFx4PCR8S8KO4QVZVVFJFWVJGXV06XkZLQUJOREJZQU9JIV5RXB7j4vcRBQYfRVJfRFpeQUwOHAUbBwERE1VCVElVT1FcDPDk5wdGX1ZJW0VICg8ZBxkZBQtRTlcfGScPHxMVHhIaovyuGEYJBxMBHR8PCU9UW0BfRUtFR0FGVFsWFBIaFwv15edBXkhRVEZQXFJcX1ZDUgUbHhEGAhYVCVhBXk9OTbWssltJVFYzWV9QWF9XX1xeSEZLLldaVRkaGQ4OHB0GoruxTVBXSkUJBR4CGBgKClJLX0FaTFpVCwkfHhhfRE+us7VPRgQFEwEfAx8VT1RNGR8tBREdHxQUHFgGUAZcE+H16xcRAQNFUl1aRVtVX15JREFfVhkZGR8QDg4YGFxFrbaxTV1TXl5dXFhHThkHAhUCBhIZBVRNWktKSUlQTketsLI3VVNcVF5TX19CVFtZKlNeURUWFQIKGBkCX",
    "encryptedKey": "GhULXw4QFgUMAhtCQVdSSFpOT0hOBAgUDBQJFhINWRgLEQQeBwARE1lPVVIOCwgfHxFHQU5aHhQNHR8PC1FLREBETEhGUkxJWFcYAAYTEw0bGAsJTVhTSkAJAQECGVBGUEhQRlhCQlgOCgkOAAQJBFhFWRsHJBMMChUGFVRdURYQFwcMDAUBABhMRkhVQw8HDAgGCldHREBaSkpCUhkWAwIJABMTB1pHWFdHWUJJVQoRGwYNBRMLB1lAUFpJQkdCUAUeGh8CAw8eGEZLQUNXREFSXVhPTEtZT1lXXQcMHRAUCQYOClNAUFlPQwUFEEVCQUJdR0xZVkkMEQkTDQ0MEFgfUBJQTBAoAAkBA1NJSENWSEVZVE4CEwgZHBsSGVVET0dOS0lQR0YZDQ8NARYWFl5KGAgcJhYNHh8RRUBBVkpeWEhBUQcKGh8NAwcHF1pAQEJNQUtYXRsRGxIIAwsHH0VQQ1VGSUJBQgsQGhADHR8PGVhFQlZHQkFQUk1IVUhbUU1SFhAJGRISDwcfQkpLWEtRRE5CBA==",
    "algorithm": "AES-256-GCM + RSA-PKCS1",
    "researchSite": "gauteng",
    "timestamp": "2025-08-11T21:46:12.639249"
}

if __name__ == "__main__":
    # Test decryption
    private_key_path = "../pipeline_toolkit/secrets/private_key.pem"
    # Use production passphrase in production
    passphrase = input("Enter private key passphrase: ")
    
    print("Testing hybrid AES/RSA decryption...")
    print("=" * 50)
    
    result = decrypt_hybrid_format(test_hybrid_data, private_key_path, passphrase)
    
    if result:
        print("\n" + "=" * 50)
        print("HYBRID DECRYPTION SUCCESSFUL!")
        print("=" * 50)
        print(f"Decrypted data:")
        print(json.dumps(result, indent=2))
    else:
        print("❌ Hybrid decryption failed")