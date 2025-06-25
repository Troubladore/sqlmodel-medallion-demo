#!/usr/bin/env python3
"""
Test script for the new temporal table auto-discovery system.

This script validates the auto-discovery logic without actually creating triggers,
making it safe to run for testing and debugging.
"""

import sys
import os
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.utils import configure_logging
from src._base.temporal_discovery import (
    discover_temporal_tables_for_database,
    print_temporal_discovery_summary,
    analyze_temporal_table_pair,
    generate_trigger_config
)

def test_temporal_discovery():
    """Test the temporal table discovery system"""
    
    configure_logging("INFO")
    
    print("🧪 TESTING TEMPORAL TABLE AUTO-DISCOVERY SYSTEM")
    print("=" * 80)
    
    src_dir = os.path.join(str(Path(__file__).parent.parent), "src")
    database_name = "demo"  # Test with demo database
    
    # Print detailed discovery summary
    print_temporal_discovery_summary(src_dir, database_name)
    
    # Test the discovery
    temporal_pairs = discover_temporal_tables_for_database(src_dir, database_name)
    
    if not temporal_pairs:
        print("❌ No temporal table pairs found!")
        return False
    
    print(f"\n🎯 DISCOVERED TEMPORAL PAIRS: {len(temporal_pairs)}")
    print("-" * 50)
    
    for i, pair in enumerate(temporal_pairs, 1):
        print(f"\n{i}. {pair.base_name}")
        print(f"   Primary: {pair.primary_table_name}")
        print(f"   History: {pair.history_table_name}")
        print(f"   Schema: {pair.schema_name}")
        
        # Analyze the pair
        analysis = analyze_temporal_table_pair(pair)
        
        if analysis.is_valid:
            print(f"   ✅ Valid temporal pair")
            
            # Generate trigger config
            config = generate_trigger_config(analysis)
            print(f"   🔧 Trigger Config:")
            print(f"      Primary key: {config.key_column}")
            print(f"      Columns: {', '.join(config.additional_columns)}")
        else:
            print(f"   ❌ Invalid temporal pair:")
            for message in analysis.validation_messages:
                print(f"      • {message}")
    
    print(f"\n🎉 Discovery test completed! Found {len(temporal_pairs)} temporal pairs.")
    return len(temporal_pairs) > 0

if __name__ == "__main__":
    success = test_temporal_discovery()
    sys.exit(0 if success else 1)