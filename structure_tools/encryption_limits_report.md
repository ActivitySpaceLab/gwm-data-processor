# Encryption Size Limits Analysis Report

Generated on: 2025-09-17T10:59:07.690131

## Executive Summary

This report analyzes the performance and size limits of the hybrid AES/RSA encryption system used in the Gauteng Wellbeing Mapper for encrypting GPS location tracking data.

## Test Scenarios

| Scenario | GPS Frequency | Duration | Expected Points | Data Collection Rate |
|----------|---------------|----------|----------------|-----------------------|
| 1 | 900 sec | 336 hours | 1,344 | 96/day |
| 2 | 300 sec | 336 hours | 4,032 | 288/day |
| 3 | 60 sec | 336 hours | 20,160 | 1440/day |
| 4 | 30 sec | 336 hours | 40,320 | 2880/day |
| 5 | 10 sec | 336 hours | 120,960 | 8640/day |
| 6 | 5 sec | 336 hours | 241,920 | 17280/day |

## Detailed Results

### Scenario 1: Low frequency (15 min intervals)

**Description**: 2-week tracking with 15-minute GPS intervals

#### AES_CBC Encryption

| Metric | Value |
|--------|-------|
| Location Points | 1,344 |
| Original Size | 299,496 bytes (0.29 MB) |
| Encrypted Size | 399,802 bytes (0.38 MB) |
| Size Increase | 1.33x |
| Encryption Time | 0.00 seconds |
| Encryption Memory | 3.12 MB |
| Decryption Time | 0.00 seconds |
| Decryption Memory | 0.89 MB |
| Decryption Success | ✅ Yes |

#### XOR Encryption

| Metric | Value |
|--------|-------|
| Location Points | 1,344 |
| Original Size | 299,496 bytes (0.29 MB) |
| Encrypted Size | 399,766 bytes (0.38 MB) |
| Size Increase | 1.33x |
| Encryption Time | 0.02 seconds |
| Encryption Memory | 4.58 MB |
| Decryption Time | 0.02 seconds |
| Decryption Memory | 3.16 MB |
| Decryption Success | ✅ Yes |

### Scenario 2: Medium frequency (5 min intervals)

**Description**: 2-week tracking with 5-minute GPS intervals

#### AES_CBC Encryption

| Metric | Value |
|--------|-------|
| Location Points | 4,032 |
| Original Size | 897,917 bytes (0.86 MB) |
| Encrypted Size | 1,197,690 bytes (1.14 MB) |
| Size Increase | 1.33x |
| Encryption Time | 0.01 seconds |
| Encryption Memory | 7.92 MB |
| Decryption Time | 0.01 seconds |
| Decryption Memory | 5.70 MB |
| Decryption Success | ✅ Yes |

#### XOR Encryption

| Metric | Value |
|--------|-------|
| Location Points | 4,032 |
| Original Size | 897,917 bytes (0.86 MB) |
| Encrypted Size | 1,197,662 bytes (1.14 MB) |
| Size Increase | 1.33x |
| Encryption Time | 0.06 seconds |
| Encryption Memory | 14.23 MB |
| Decryption Time | 0.06 seconds |
| Decryption Memory | 11.72 MB |
| Decryption Success | ✅ Yes |

### Scenario 3: High frequency (1 min intervals)

**Description**: 2-week tracking with 1-minute GPS intervals

#### AES_CBC Encryption

| Metric | Value |
|--------|-------|
| Location Points | 20,160 |
| Original Size | 4,488,023 bytes (4.28 MB) |
| Encrypted Size | 5,984,506 bytes (5.71 MB) |
| Size Increase | 1.33x |
| Encryption Time | 0.07 seconds |
| Encryption Memory | 38.84 MB |
| Decryption Time | 0.04 seconds |
| Decryption Memory | 11.55 MB |
| Decryption Success | ✅ Yes |

#### XOR Encryption

| Metric | Value |
|--------|-------|
| Location Points | 20,160 |
| Original Size | 4,488,023 bytes (4.28 MB) |
| Encrypted Size | 5,984,470 bytes (5.71 MB) |
| Size Increase | 1.33x |
| Encryption Time | 0.30 seconds |
| Encryption Memory | 28.30 MB |
| Decryption Time | 0.28 seconds |
| Decryption Memory | 6.03 MB |
| Decryption Success | ✅ Yes |

### Scenario 4: Very high frequency (30 sec intervals)

**Description**: 2-week tracking with 30-second GPS intervals

#### AES_CBC Encryption

| Metric | Value |
|--------|-------|
| Location Points | 40,320 |
| Original Size | 8,976,351 bytes (8.56 MB) |
| Encrypted Size | 11,968,934 bytes (11.41 MB) |
| Size Increase | 1.33x |
| Encryption Time | 0.13 seconds |
| Encryption Memory | 82.97 MB |
| Decryption Time | 0.08 seconds |
| Decryption Memory | 23.09 MB |
| Decryption Success | ✅ Yes |

#### XOR Encryption

| Metric | Value |
|--------|-------|
| Location Points | 40,320 |
| Original Size | 8,976,351 bytes (8.56 MB) |
| Encrypted Size | 11,968,906 bytes (11.41 MB) |
| Size Increase | 1.33x |
| Encryption Time | 0.59 seconds |
| Encryption Memory | 11.42 MB |
| Decryption Time | 0.56 seconds |
| Decryption Memory | 0.47 MB |
| Decryption Success | ✅ Yes |

### Scenario 5: Extreme frequency (10 sec intervals)

**Description**: 2-week tracking with 10-second GPS intervals

#### AES_CBC Encryption

| Metric | Value |
|--------|-------|
| Location Points | 120,960 |
| Original Size | 26,927,066 bytes (25.68 MB) |
| Encrypted Size | 35,903,226 bytes (34.24 MB) |
| Size Increase | 1.33x |
| Encryption Time | 0.39 seconds |
| Encryption Memory | 201.58 MB |
| Decryption Time | 0.26 seconds |
| Decryption Memory | 69.27 MB |
| Decryption Success | ✅ Yes |

#### XOR Encryption

| Metric | Value |
|--------|-------|
| Location Points | 120,960 |
| Original Size | 26,927,066 bytes (25.68 MB) |
| Encrypted Size | 35,903,194 bytes (34.24 MB) |
| Size Increase | 1.33x |
| Encryption Time | 1.79 seconds |
| Encryption Memory | 36.25 MB |
| Decryption Time | 1.68 seconds |
| Decryption Memory | 35.25 MB |
| Decryption Success | ✅ Yes |

### Scenario 6: Maximum frequency (5 sec intervals)

**Description**: 2-week tracking with 5-second GPS intervals

#### AES_CBC Encryption

| Metric | Value |
|--------|-------|
| Location Points | 241,920 |
| Original Size | 53,852,946 bytes (51.36 MB) |
| Encrypted Size | 71,804,410 bytes (68.48 MB) |
| Size Increase | 1.33x |
| Encryption Time | 0.78 seconds |
| Encryption Memory | 364.22 MB |
| Decryption Time | 0.52 seconds |
| Decryption Memory | 138.34 MB |
| Decryption Success | ✅ Yes |

#### XOR Encryption

| Metric | Value |
|--------|-------|
| Location Points | 241,920 |
| Original Size | 53,852,946 bytes (51.36 MB) |
| Encrypted Size | 71,804,366 bytes (68.48 MB) |
| Size Increase | 1.33x |
| Encryption Time | 3.58 seconds |
| Encryption Memory | 52.59 MB |
| Decryption Time | 3.34 seconds |
| Decryption Memory | 4.08 MB |
| Decryption Success | ✅ Yes |

## Performance Analysis

### Size Limits

- **Maximum tested location points**: 241,920
- **Maximum encrypted size**: 68.48 MB
- **Maximum encryption time**: 3.58 seconds

### Encryption Method Comparison

| Method | Avg Size Increase | Avg Encryption Time | Avg Decryption Time | Security Level |
|--------|-------------------|---------------------|---------------------|----------------|
| AES_CBC | 1.33x | 0.23s | 0.15s | High (AES-256-CBC + RSA-2048) |
| XOR | 1.33x | 1.06s | 0.99s | Medium (XOR + RSA-2048) |

## Recommendations

### For Production Use

Based on the analysis results:

1. **Recommended GPS frequency**: 1-5 minute intervals for 2-week studies
2. **Maximum practical data points**: ~50,000 for 2-week studies
3. **Preferred encryption method**: AES-CBC for security, XOR for legacy compatibility
4. **Expected data sizes**: 1-10 MB for typical 2-week high-resolution tracks

### Performance Optimization

1. **Compression**: Consider data compression before encryption
2. **Chunking**: Split large datasets into smaller encrypted chunks
3. **Adaptive sampling**: Reduce frequency during stationary periods
4. **Background processing**: Encrypt data incrementally, not all at once

### Security Considerations

1. **AES-CBC preferred**: More secure than XOR method
2. **Key management**: Ensure proper RSA key storage and rotation
3. **Data integrity**: Verify decrypted data matches original
4. **Performance vs Security**: Balance based on study requirements

## Technical Implementation

### Encryption Process

1. Location data is JSON-serialized
2. Random 256-bit AES key is generated
3. Data is encrypted with AES (CBC mode with PKCS7 padding)
4. AES key is encrypted with RSA-2048 (OAEP padding)
5. Both encrypted data and key are base64-encoded
6. Final package is JSON-serialized for transmission

### Size Growth Factors

- **Base64 encoding**: ~33% size increase
- **AES padding**: Up to 16 bytes per encryption
- **JSON structure**: Metadata and formatting overhead
- **RSA encrypted key**: Fixed 256 bytes (base64: ~344 bytes)

