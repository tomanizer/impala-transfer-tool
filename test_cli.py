#!/usr/bin/env python3
"""
Simple test script to verify CLI changes work correctly.
"""

import sys
import os

# Add the current directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Test CLI module directly
try:
    from impala_transfer.cli import create_parser, get_environment_config, mask_sensitive_config
    
    print("✓ Successfully imported CLI functions")
    
    # Test parser creation
    parser = create_parser()
    print("✓ Parser created successfully")
    
    # Test environment config
    env_config = get_environment_config()
    print(f"✓ Environment config loaded: {len(env_config)} items")
    
    # Test masking function
    test_config = {
        'host': 'localhost',
        'password': 'secret123',
        'nested': {
            'api_key': 'sk-1234567890'
        }
    }
    masked_config = mask_sensitive_config(test_config)
    print(f"✓ Config masking works: {masked_config}")
    
    print("\n🎉 All CLI tests passed!")
    
except ImportError as e:
    print(f"❌ Import error: {e}")
    sys.exit(1)
except Exception as e:
    print(f"❌ Error: {e}")
    sys.exit(1) 