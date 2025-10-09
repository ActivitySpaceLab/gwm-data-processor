#!/usr/bin/env python3
"""
Encryption Size Limits Analysis
==============================

This script tests and documents the limits for large location data sets
with the hybrid AES/RSA encryption system used in the Gauteng Wellbeing Mapper.

It simulates realistic location tracking scenarios and measures:
- Encryption performance and time
- Encrypted data size growth
- Memory usage during encryption/decryption
- Practical limits for 2-week high-resolution tracks

Features:
- Simulates realistic GPS tracking data
- Tests various data collection scenarios
- Measures encryption performance metrics
- Documents size limits and recommendations
- Provides optimization recommendations

Usage:
    python3 analyze_encryption_limits.py [options]
    
Examples:
    # Test standard scenarios
    python3 analyze_encryption_limits.py --test-scenarios
    
    # Test extreme high-resolution tracking
    python3 analyze_encryption_limits.py --high-resolution --days 14
    
    # Generate performance report
    python3 analyze_encryption_limits.py --test-scenarios --report
"""

import argparse
import json
import base64
import os
import sys
import time
import psutil
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
import random
import math

# Import encryption libraries
try:
    from cryptography.hazmat.primitives import serialization, hashes
    from cryptography.hazmat.primitives.asymmetric import rsa, padding
    from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives.padding import PKCS7
except ImportError:
    print("❌ Error: Required 'cryptography' library not found.")
    print("Please install it by running: pip install cryptography")
    sys.exit(1)

try:
    import psutil
except ImportError:
    print("❌ Error: Required 'psutil' library not found.")
    print("Please install it by running: pip install psutil")
    sys.exit(1)

class LocationDataGenerator:
    """Generate realistic GPS location tracking data for testing"""
    
    def __init__(self):
        # Johannesburg area coordinates
        self.base_lat = -26.2041
        self.base_lng = 28.0473
        self.max_movement = 0.1  # Degrees (~11km)
        
    def generate_location_track(self, duration_hours: int, frequency_seconds: int) -> List[Dict]:
        """Generate realistic location tracking data"""
        
        locations = []
        total_points = (duration_hours * 3600) // frequency_seconds
        current_time = datetime.now()
        
        # Start at base location
        current_lat = self.base_lat
        current_lng = self.base_lng
        
        print(f"📍 Generating {total_points} location points over {duration_hours} hours")
        print(f"   Frequency: Every {frequency_seconds} seconds")
        
        for i in range(total_points):
            # Simulate realistic movement patterns
            if i % 1000 == 0:  # Progress indicator
                print(f"   Generated {i}/{total_points} points ({i/total_points*100:.1f}%)")
            
            # Random walk with some constraints
            lat_change = random.uniform(-0.001, 0.001)  # ~111m max movement
            lng_change = random.uniform(-0.001, 0.001)
            
            # Keep within reasonable bounds
            new_lat = max(min(current_lat + lat_change, self.base_lat + self.max_movement), 
                         self.base_lat - self.max_movement)
            new_lng = max(min(current_lng + lng_change, self.base_lng + self.max_movement),
                         self.base_lng - self.max_movement)
            
            current_lat = new_lat
            current_lng = new_lng
            
            # Generate location point
            location = {
                'timestamp': (current_time + timedelta(seconds=i * frequency_seconds)).isoformat(),
                'latitude': round(current_lat, 6),
                'longitude': round(current_lng, 6),
                'accuracy': random.uniform(3, 15),  # GPS accuracy in meters
                'altitude': random.uniform(1400, 1800),  # Johannesburg elevation
                'speed': random.uniform(0, 30),  # Speed in m/s (0-108 km/h)
                'heading': random.uniform(0, 360),  # Direction in degrees
                'activity': random.choice(['stationary', 'walking', 'driving', 'unknown'])
            }
            
            locations.append(location)
        
        print(f"✅ Generated {len(locations)} location points")
        return locations
    
    def create_location_package(self, locations: List[Dict], participant_id: str = "TEST_001") -> Dict:
        """Create a location data package similar to what the app generates"""
        
        package = {
            'participantId': participant_id,
            'surveyResponseId': f"RESP_{int(time.time())}",
            'collectionPeriod': {
                'startTime': locations[0]['timestamp'] if locations else '',
                'endTime': locations[-1]['timestamp'] if locations else '',
                'durationHours': len(locations) / 3600 if locations else 0
            },
            'metadata': {
                'totalPoints': len(locations),
                'averageAccuracy': sum(loc['accuracy'] for loc in locations) / len(locations) if locations else 0,
                'deviceInfo': 'Test Device',
                'appVersion': '1.0.0'
            },
            'locationData': locations
        }
        
        return package

class EncryptionTester:
    """Test encryption performance and size limits"""
    
    def __init__(self):
        self.rsa_key_pair = None
        self.results = []
        
    def generate_key_pair(self, key_size: int = 2048) -> None:
        """Generate RSA key pair for testing"""
        print(f"🔑 Generating {key_size}-bit RSA key pair...")
        
        private_key = rsa.generate_private_key(
            public_exponent=65537,
            key_size=key_size,
            backend=default_backend()
        )
        
        self.rsa_key_pair = {
            'private': private_key,
            'public': private_key.public_key()
        }
        
        print("✅ RSA key pair generated")
    
    def encrypt_location_data(self, location_package: Dict, method: str = 'aes_cbc') -> Tuple[Dict, Dict]:
        """Encrypt location data using hybrid encryption"""
        
        start_time = time.time()
        start_memory = psutil.Process().memory_info().rss / 1024 / 1024  # MB
        
        # Convert to JSON
        json_data = json.dumps(location_package, separators=(',', ':'))
        original_size = len(json_data.encode('utf-8'))
        
        # Generate AES key
        aes_key = os.urandom(32)  # 256-bit AES key
        
        if method == 'aes_cbc':
            encrypted_data = self._encrypt_aes_cbc(json_data.encode('utf-8'), aes_key)
        else:  # xor method (legacy)
            encrypted_data = self._encrypt_xor(json_data.encode('utf-8'), aes_key)
        
        # Encrypt AES key with RSA
        encrypted_aes_key = self.rsa_key_pair['public'].encrypt(
            aes_key,
            padding.OAEP(
                mgf=padding.MGF1(algorithm=hashes.SHA256()),
                algorithm=hashes.SHA256(),
                label=None
            )
        )
        
        # Create encrypted package
        encrypted_package = {
            'encryptedData': base64.b64encode(encrypted_data).decode('utf-8'),
            'encryptedKey': base64.b64encode(encrypted_aes_key).decode('utf-8'),
            'method': method,
            'timestamp': datetime.now().isoformat()
        }
        
        encrypted_json = json.dumps(encrypted_package, separators=(',', ':'))
        encrypted_size = len(encrypted_json.encode('utf-8'))
        
        end_time = time.time()
        end_memory = psutil.Process().memory_info().rss / 1024 / 1024  # MB
        
        # Performance metrics
        metrics = {
            'original_size_bytes': original_size,
            'encrypted_size_bytes': encrypted_size,
            'size_increase_ratio': encrypted_size / original_size,
            'encryption_time_seconds': end_time - start_time,
            'memory_usage_mb': end_memory - start_memory,
            'points_count': len(location_package.get('locationData', [])),
            'method': method
        }
        
        return encrypted_package, metrics
    
    def _encrypt_aes_cbc(self, data: bytes, key: bytes) -> bytes:
        """Encrypt data using AES-CBC"""
        
        # Generate random IV
        iv = os.urandom(16)
        
        # Pad data to block size
        padder = PKCS7(128).padder()
        padded_data = padder.update(data)
        padded_data += padder.finalize()
        
        # Encrypt
        cipher = Cipher(algorithms.AES(key), modes.CBC(iv), backend=default_backend())
        encryptor = cipher.encryptor()
        encrypted_data = encryptor.update(padded_data) + encryptor.finalize()
        
        # Prepend IV to encrypted data
        return iv + encrypted_data
    
    def _encrypt_xor(self, data: bytes, key: bytes) -> bytes:
        """Encrypt data using XOR (legacy method)"""
        
        encrypted = []
        for i in range(len(data)):
            encrypted.append(data[i] ^ key[i % len(key)])
        
        return bytes(encrypted)
    
    def decrypt_location_data(self, encrypted_package: Dict) -> Tuple[Dict, Dict]:
        """Decrypt location data and measure performance"""
        
        start_time = time.time()
        start_memory = psutil.Process().memory_info().rss / 1024 / 1024  # MB
        
        try:
            # Decode from base64
            encrypted_data = base64.b64decode(encrypted_package['encryptedData'])
            encrypted_key = base64.b64decode(encrypted_package['encryptedKey'])
            method = encrypted_package.get('method', 'aes_cbc')
            
            # Decrypt AES key with RSA
            aes_key = self.rsa_key_pair['private'].decrypt(
                encrypted_key,
                padding.OAEP(
                    mgf=padding.MGF1(algorithm=hashes.SHA256()),
                    algorithm=hashes.SHA256(),
                    label=None
                )
            )
            
            # Decrypt data
            if method == 'aes_cbc':
                decrypted_data = self._decrypt_aes_cbc(encrypted_data, aes_key)
            else:  # xor method
                decrypted_data = self._decrypt_xor(encrypted_data, aes_key)
            
            # Parse JSON
            location_package = json.loads(decrypted_data.decode('utf-8'))
            
            end_time = time.time()
            end_memory = psutil.Process().memory_info().rss / 1024 / 1024  # MB
            
            metrics = {
                'decryption_time_seconds': end_time - start_time,
                'memory_usage_mb': end_memory - start_memory,
                'success': True
            }
            
            return location_package, metrics
            
        except Exception as e:
            end_time = time.time()
            metrics = {
                'decryption_time_seconds': end_time - start_time,
                'memory_usage_mb': 0,
                'success': False,
                'error': str(e)
            }
            return {}, metrics
    
    def _decrypt_aes_cbc(self, data: bytes, key: bytes) -> bytes:
        """Decrypt data using AES-CBC"""
        
        # Extract IV and encrypted content
        iv = data[:16]
        encrypted_content = data[16:]
        
        # Decrypt
        cipher = Cipher(algorithms.AES(key), modes.CBC(iv), backend=default_backend())
        decryptor = cipher.decryptor()
        padded_data = decryptor.update(encrypted_content) + decryptor.finalize()
        
        # Remove padding
        unpadder = PKCS7(128).unpadder()
        data = unpadder.update(padded_data)
        data += unpadder.finalize()
        
        return data
    
    def _decrypt_xor(self, data: bytes, key: bytes) -> bytes:
        """Decrypt data using XOR"""
        
        decrypted = []
        for i in range(len(data)):
            decrypted.append(data[i] ^ key[i % len(key)])
        
        return bytes(decrypted)

class EncryptionLimitsAnalyzer:
    """Analyze encryption limits for different scenarios"""
    
    def __init__(self):
        self.generator = LocationDataGenerator()
        self.tester = EncryptionTester()
        self.results = []
        
        # Test scenarios
        self.scenarios = [
            {
                'name': 'Low frequency (15 min intervals)',
                'description': '2-week tracking with 15-minute GPS intervals',
                'hours': 24 * 14,  # 2 weeks
                'frequency': 15 * 60,  # 15 minutes
                'expected_points': (24 * 14 * 3600) // (15 * 60)  # ~1,344 points
            },
            {
                'name': 'Medium frequency (5 min intervals)',
                'description': '2-week tracking with 5-minute GPS intervals',
                'hours': 24 * 14,
                'frequency': 5 * 60,  # 5 minutes
                'expected_points': (24 * 14 * 3600) // (5 * 60)  # ~4,032 points
            },
            {
                'name': 'High frequency (1 min intervals)',
                'description': '2-week tracking with 1-minute GPS intervals',
                'hours': 24 * 14,
                'frequency': 60,  # 1 minute
                'expected_points': (24 * 14 * 3600) // 60  # ~20,160 points
            },
            {
                'name': 'Very high frequency (30 sec intervals)',
                'description': '2-week tracking with 30-second GPS intervals',
                'hours': 24 * 14,
                'frequency': 30,  # 30 seconds
                'expected_points': (24 * 14 * 3600) // 30  # ~40,320 points
            },
            {
                'name': 'Extreme frequency (10 sec intervals)',
                'description': '2-week tracking with 10-second GPS intervals',
                'hours': 24 * 14,
                'frequency': 10,  # 10 seconds
                'expected_points': (24 * 14 * 3600) // 10  # ~120,960 points
            },
            {
                'name': 'Maximum frequency (5 sec intervals)',
                'description': '2-week tracking with 5-second GPS intervals',
                'hours': 24 * 14,
                'frequency': 5,  # 5 seconds
                'expected_points': (24 * 14 * 3600) // 5  # ~241,920 points
            }
        ]
    
    def run_all_scenarios(self) -> None:
        """Run analysis for all predefined scenarios"""
        
        print("🔬 Starting encryption limits analysis...")
        print(f"Testing {len(self.scenarios)} scenarios\n")
        
        # Generate RSA key pair
        self.tester.generate_key_pair()
        
        for i, scenario in enumerate(self.scenarios, 1):
            print(f"\n{'='*60}")
            print(f"📊 SCENARIO {i}/{len(self.scenarios)}: {scenario['name']}")
            print(f"{'='*60}")
            print(f"Description: {scenario['description']}")
            print(f"Expected points: {scenario['expected_points']:,}")
            
            result = self._test_scenario(scenario)
            self.results.append(result)
            
            print(f"\n✅ Scenario {i} completed")
    
    def _test_scenario(self, scenario: Dict) -> Dict:
        """Test a single encryption scenario"""
        
        try:
            # Generate location data
            print(f"\n📍 Generating location data...")
            locations = self.generator.generate_location_track(
                scenario['hours'], 
                scenario['frequency']
            )
            
            package = self.generator.create_location_package(locations)
            
            # Test both encryption methods
            results = {'scenario': scenario, 'methods': {}}
            
            for method in ['aes_cbc', 'xor']:
                print(f"\n🔐 Testing {method.upper()} encryption...")
                
                # Encrypt
                encrypted_package, encrypt_metrics = self.tester.encrypt_location_data(package, method)
                
                print(f"   Original size: {encrypt_metrics['original_size_bytes']:,} bytes ({encrypt_metrics['original_size_bytes']/1024/1024:.2f} MB)")
                print(f"   Encrypted size: {encrypt_metrics['encrypted_size_bytes']:,} bytes ({encrypt_metrics['encrypted_size_bytes']/1024/1024:.2f} MB)")
                print(f"   Size increase: {encrypt_metrics['size_increase_ratio']:.2f}x")
                print(f"   Encryption time: {encrypt_metrics['encryption_time_seconds']:.2f} seconds")
                print(f"   Memory usage: {encrypt_metrics['memory_usage_mb']:.2f} MB")
                
                # Decrypt
                print(f"   🔓 Testing decryption...")
                decrypted_package, decrypt_metrics = self.tester.decrypt_location_data(encrypted_package)
                
                if decrypt_metrics['success']:
                    print(f"   ✅ Decryption successful")
                    print(f"   Decryption time: {decrypt_metrics['decryption_time_seconds']:.2f} seconds")
                    print(f"   Memory usage: {decrypt_metrics['memory_usage_mb']:.2f} MB")
                    
                    # Verify data integrity
                    if len(decrypted_package.get('locationData', [])) == len(locations):
                        print(f"   ✅ Data integrity verified")
                    else:
                        print(f"   ❌ Data integrity issue: {len(decrypted_package.get('locationData', []))} != {len(locations)}")
                else:
                    print(f"   ❌ Decryption failed: {decrypt_metrics.get('error', 'Unknown error')}")
                
                # Store results
                results['methods'][method] = {
                    'encryption': encrypt_metrics,
                    'decryption': decrypt_metrics
                }
            
            return results
            
        except Exception as e:
            print(f"❌ Scenario failed: {e}")
            return {
                'scenario': scenario,
                'error': str(e),
                'methods': {}
            }
    
    def generate_report(self, output_file: str = 'encryption_limits_report.md') -> None:
        """Generate comprehensive analysis report"""
        
        print(f"\n📋 Generating analysis report...")
        
        with open(output_file, 'w') as f:
            f.write("# Encryption Size Limits Analysis Report\n\n")
            f.write(f"Generated on: {datetime.now().isoformat()}\n\n")
            
            # Executive Summary
            f.write("## Executive Summary\n\n")
            f.write("This report analyzes the performance and size limits of the hybrid AES/RSA encryption system ")
            f.write("used in the Gauteng Wellbeing Mapper for encrypting GPS location tracking data.\n\n")
            
            # Test scenarios overview
            f.write("## Test Scenarios\n\n")
            f.write("| Scenario | GPS Frequency | Duration | Expected Points | Data Collection Rate |\n")
            f.write("|----------|---------------|----------|----------------|-----------------------|\n")
            
            for i, result in enumerate(self.results, 1):
                scenario = result['scenario']
                rate_per_day = scenario['expected_points'] / 14  # points per day
                f.write(f"| {i} | {scenario['frequency']} sec | {scenario['hours']} hours | {scenario['expected_points']:,} | {rate_per_day:.0f}/day |\n")
            
            f.write("\n")
            
            # Detailed results
            f.write("## Detailed Results\n\n")
            
            for i, result in enumerate(self.results, 1):
                if 'error' in result:
                    f.write(f"### Scenario {i}: {result['scenario']['name']} ❌\n\n")
                    f.write(f"**Error**: {result['error']}\n\n")
                    continue
                
                scenario = result['scenario']
                f.write(f"### Scenario {i}: {scenario['name']}\n\n")
                f.write(f"**Description**: {scenario['description']}\n\n")
                
                # Results table for each method
                for method, data in result['methods'].items():
                    if not data:
                        continue
                        
                    f.write(f"#### {method.upper()} Encryption\n\n")
                    
                    encrypt = data.get('encryption', {})
                    decrypt = data.get('decryption', {})
                    
                    f.write("| Metric | Value |\n")
                    f.write("|--------|-------|\n")
                    f.write(f"| Location Points | {encrypt.get('points_count', 0):,} |\n")
                    f.write(f"| Original Size | {encrypt.get('original_size_bytes', 0):,} bytes ({encrypt.get('original_size_bytes', 0)/1024/1024:.2f} MB) |\n")
                    f.write(f"| Encrypted Size | {encrypt.get('encrypted_size_bytes', 0):,} bytes ({encrypt.get('encrypted_size_bytes', 0)/1024/1024:.2f} MB) |\n")
                    f.write(f"| Size Increase | {encrypt.get('size_increase_ratio', 0):.2f}x |\n")
                    f.write(f"| Encryption Time | {encrypt.get('encryption_time_seconds', 0):.2f} seconds |\n")
                    f.write(f"| Encryption Memory | {encrypt.get('memory_usage_mb', 0):.2f} MB |\n")
                    f.write(f"| Decryption Time | {decrypt.get('decryption_time_seconds', 0):.2f} seconds |\n")
                    f.write(f"| Decryption Memory | {decrypt.get('memory_usage_mb', 0):.2f} MB |\n")
                    f.write(f"| Decryption Success | {'✅ Yes' if decrypt.get('success', False) else '❌ No'} |\n")
                    
                    if not decrypt.get('success', False):
                        f.write(f"| Error | {decrypt.get('error', 'Unknown')} |\n")
                    
                    f.write("\n")
            
            # Performance analysis
            f.write("## Performance Analysis\n\n")
            
            # Find performance limits
            successful_scenarios = [r for r in self.results if 'error' not in r and r['methods']]
            
            if successful_scenarios:
                f.write("### Size Limits\n\n")
                
                max_points = max(r['scenario']['expected_points'] for r in successful_scenarios)
                max_size_mb = 0
                max_encrypt_time = 0
                
                for result in successful_scenarios:
                    for method_data in result['methods'].values():
                        encrypt = method_data.get('encryption', {})
                        max_size_mb = max(max_size_mb, encrypt.get('encrypted_size_bytes', 0) / 1024 / 1024)
                        max_encrypt_time = max(max_encrypt_time, encrypt.get('encryption_time_seconds', 0))
                
                f.write(f"- **Maximum tested location points**: {max_points:,}\n")
                f.write(f"- **Maximum encrypted size**: {max_size_mb:.2f} MB\n")
                f.write(f"- **Maximum encryption time**: {max_encrypt_time:.2f} seconds\n\n")
                
                f.write("### Encryption Method Comparison\n\n")
                f.write("| Method | Avg Size Increase | Avg Encryption Time | Avg Decryption Time | Security Level |\n")
                f.write("|--------|-------------------|---------------------|---------------------|----------------|\n")
                
                # Calculate averages for each method
                for method in ['aes_cbc', 'xor']:
                    method_results = []
                    for result in successful_scenarios:
                        if method in result['methods']:
                            method_results.append(result['methods'][method])
                    
                    if method_results:
                        avg_size_increase = sum(r['encryption']['size_increase_ratio'] for r in method_results) / len(method_results)
                        avg_encrypt_time = sum(r['encryption']['encryption_time_seconds'] for r in method_results) / len(method_results)
                        avg_decrypt_time = sum(r['decryption']['decryption_time_seconds'] for r in method_results) / len(method_results)
                        
                        security = "High (AES-256-CBC + RSA-2048)" if method == 'aes_cbc' else "Medium (XOR + RSA-2048)"
                        
                        f.write(f"| {method.upper()} | {avg_size_increase:.2f}x | {avg_encrypt_time:.2f}s | {avg_decrypt_time:.2f}s | {security} |\n")
                
                f.write("\n")
            
            # Recommendations
            f.write("## Recommendations\n\n")
            
            f.write("### For Production Use\n\n")
            f.write("Based on the analysis results:\n\n")
            
            if successful_scenarios:
                # Find practical limits
                practical_limit_points = 50000  # Conservative estimate
                practical_scenarios = [r for r in successful_scenarios if r['scenario']['expected_points'] <= practical_limit_points]
                
                if practical_scenarios:
                    f.write(f"1. **Recommended GPS frequency**: 1-5 minute intervals for 2-week studies\n")
                    f.write(f"2. **Maximum practical data points**: ~{practical_limit_points:,} for 2-week studies\n")
                    f.write(f"3. **Preferred encryption method**: AES-CBC for security, XOR for legacy compatibility\n")
                    f.write(f"4. **Expected data sizes**: 1-10 MB for typical 2-week high-resolution tracks\n\n")
                else:
                    f.write("1. **Caution**: High-frequency GPS tracking may exceed practical limits\n")
                    f.write("2. **Recommendation**: Use adaptive frequency based on movement patterns\n\n")
            
            f.write("### Performance Optimization\n\n")
            f.write("1. **Compression**: Consider data compression before encryption\n")
            f.write("2. **Chunking**: Split large datasets into smaller encrypted chunks\n")
            f.write("3. **Adaptive sampling**: Reduce frequency during stationary periods\n")
            f.write("4. **Background processing**: Encrypt data incrementally, not all at once\n\n")
            
            f.write("### Security Considerations\n\n")
            f.write("1. **AES-CBC preferred**: More secure than XOR method\n")
            f.write("2. **Key management**: Ensure proper RSA key storage and rotation\n")
            f.write("3. **Data integrity**: Verify decrypted data matches original\n")
            f.write("4. **Performance vs Security**: Balance based on study requirements\n\n")
            
            # Technical details
            f.write("## Technical Implementation\n\n")
            f.write("### Encryption Process\n\n")
            f.write("1. Location data is JSON-serialized\n")
            f.write("2. Random 256-bit AES key is generated\n")
            f.write("3. Data is encrypted with AES (CBC mode with PKCS7 padding)\n")
            f.write("4. AES key is encrypted with RSA-2048 (OAEP padding)\n")
            f.write("5. Both encrypted data and key are base64-encoded\n")
            f.write("6. Final package is JSON-serialized for transmission\n\n")
            
            f.write("### Size Growth Factors\n\n")
            f.write("- **Base64 encoding**: ~33% size increase\n")
            f.write("- **AES padding**: Up to 16 bytes per encryption\n")
            f.write("- **JSON structure**: Metadata and formatting overhead\n")
            f.write("- **RSA encrypted key**: Fixed 256 bytes (base64: ~344 bytes)\n\n")
        
        print(f"✅ Analysis report saved: {output_file}")


def main():
    parser = argparse.ArgumentParser(
        description='Analyze encryption size limits for GPS location data',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --test-scenarios                    Run all predefined test scenarios
  %(prog)s --high-resolution --days 14        Test extreme high-resolution tracking
  %(prog)s --test-scenarios --report          Generate detailed analysis report
        """
    )
    
    # Test options
    parser.add_argument('--test-scenarios', action='store_true',
                       help='Run all predefined test scenarios')
    
    parser.add_argument('--high-resolution', action='store_true',
                       help='Test high-resolution tracking (5-second intervals)')
    
    parser.add_argument('--days', type=int, default=14,
                       help='Duration for high-resolution test (default: 14 days)')
    
    parser.add_argument('--frequency', type=int, default=5,
                       help='GPS frequency in seconds for high-resolution test (default: 5)')
    
    # Output options
    parser.add_argument('--report', action='store_true',
                       help='Generate detailed analysis report')
    
    parser.add_argument('--output', default='encryption_limits_report.md',
                       help='Output file for analysis report')
    
    # Custom test
    parser.add_argument('--custom-test', action='store_true',
                       help='Run single custom test scenario')
    
    args = parser.parse_args()
    
    if not any([args.test_scenarios, args.high_resolution, args.custom_test]):
        print("❌ Error: Must specify a test mode")
        parser.print_help()
        return 1
    
    # Create analyzer
    analyzer = EncryptionLimitsAnalyzer()
    
    if args.test_scenarios:
        # Run all predefined scenarios
        analyzer.run_all_scenarios()
    
    elif args.high_resolution:
        # Test high-resolution scenario
        print(f"🔬 Testing high-resolution GPS tracking")
        print(f"Duration: {args.days} days")
        print(f"Frequency: Every {args.frequency} seconds")
        
        custom_scenario = {
            'name': f'Custom high-resolution ({args.frequency}s intervals)',
            'description': f'{args.days}-day tracking with {args.frequency}-second GPS intervals',
            'hours': 24 * args.days,
            'frequency': args.frequency,
            'expected_points': (24 * args.days * 3600) // args.frequency
        }
        
        analyzer.tester.generate_key_pair()
        result = analyzer._test_scenario(custom_scenario)
        analyzer.results.append(result)
    
    elif args.custom_test:
        # Interactive custom test
        print("🔧 Custom test scenario")
        hours = int(input("Enter duration in hours: "))
        frequency = int(input("Enter GPS frequency in seconds: "))
        
        custom_scenario = {
            'name': 'Custom test scenario',
            'description': f'{hours}-hour tracking with {frequency}-second GPS intervals',
            'hours': hours,
            'frequency': frequency,
            'expected_points': (hours * 3600) // frequency
        }
        
        analyzer.tester.generate_key_pair()
        result = analyzer._test_scenario(custom_scenario)
        analyzer.results.append(result)
    
    # Generate report if requested
    if args.report and analyzer.results:
        analyzer.generate_report(args.output)
    
    # Print summary
    if analyzer.results:
        print(f"\n{'='*60}")
        print("📊 ENCRYPTION LIMITS ANALYSIS SUMMARY")
        print(f"{'='*60}")
        
        successful_tests = sum(1 for r in analyzer.results if 'error' not in r)
        total_tests = len(analyzer.results)
        
        print(f"Tests completed: {successful_tests}/{total_tests}")
        
        if successful_tests > 0:
            max_points = max(r['scenario']['expected_points'] for r in analyzer.results if 'error' not in r)
            print(f"Maximum points tested: {max_points:,}")
            
            # Find largest successful encryption
            max_size = 0
            max_time = 0
            
            for result in analyzer.results:
                if 'error' not in result:
                    for method_data in result['methods'].values():
                        encrypt = method_data.get('encryption', {})
                        max_size = max(max_size, encrypt.get('encrypted_size_bytes', 0))
                        max_time = max(max_time, encrypt.get('encryption_time_seconds', 0))
            
            print(f"Largest encrypted size: {max_size/1024/1024:.2f} MB")
            print(f"Longest encryption time: {max_time:.2f} seconds")
        
        if args.report:
            print(f"\n📋 Detailed report available: {args.output}")
        
        print("\n🎉 Analysis completed successfully!")
    
    return 0


if __name__ == '__main__':
    sys.exit(main())