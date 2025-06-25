#!/usr/bin/env python3
"""
⚡ Dynamic Spark Configuration Generator

Generates optimal Spark configurations for DAGs based on detected system resources
and performance tier selection.

Usage:
    from scripts.spark_config_generator import get_spark_config
    
    spark_config = get_spark_config()  # Auto-detect
    spark_config = get_spark_config("performance")  # Force tier
"""

import os
import psutil
from typing import Dict, Optional


class SparkConfigGenerator:
    """Generate optimal Spark configurations based on system resources"""
    
    def __init__(self):
        self.total_memory_gb = round(psutil.virtual_memory().total / (1024**3), 1)
        self.cpu_count = psutil.cpu_count()
        
    def detect_performance_tier(self) -> str:
        """Auto-detect optimal performance tier"""
        if self.total_memory_gb > 32 and self.cpu_count >= 12:
            return "performance"
        elif self.total_memory_gb > 16 and self.total_memory_gb <= 32 and self.cpu_count >= 8:
            return "balanced"
        else:
            return "lite"
    
    def get_config(self, tier: Optional[str] = None) -> Dict[str, str]:
        """Get Spark configuration for specified or auto-detected tier"""
        if tier is None:
            tier = self.detect_performance_tier()
        
        # Check for environment override
        if "MEDALLION_PERFORMANCE_TIER" in os.environ:
            tier = os.environ["MEDALLION_PERFORMANCE_TIER"]
        
        configs = {
            "lite": {
                "spark.executor.memory": "512m",
                "spark.driver.memory": "512m", 
                "spark.driver.maxResultSize": "256m",
                "spark.executor.cores": "1",
                "spark.sql.adaptive.coalescePartitions.minPartitionSize": "8MB",
                "spark.sql.adaptive.advisoryPartitionSizeInBytes": "32MB",
                "spark.sql.adaptive.maxNumPostShufflePartitions": "50",
                "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
                "spark.sql.execution.arrow.pyspark.enabled": "false",  # Stability over speed
            },
            "balanced": {
                "spark.executor.memory": "1g",
                "spark.driver.memory": "1g",
                "spark.driver.maxResultSize": "512m", 
                "spark.executor.cores": "2",
                "spark.sql.adaptive.coalescePartitions.minPartitionSize": "32MB",
                "spark.sql.adaptive.advisoryPartitionSizeInBytes": "128MB",
                "spark.sql.adaptive.maxNumPostShufflePartitions": "100",
                "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
                "spark.sql.execution.arrow.pyspark.enabled": "true",
            },
            "performance": {
                "spark.executor.memory": "2g",
                "spark.driver.memory": "2g",
                "spark.driver.maxResultSize": "1g",
                "spark.executor.cores": "3", 
                "spark.sql.adaptive.coalescePartitions.minPartitionSize": "64MB",
                "spark.sql.adaptive.advisoryPartitionSizeInBytes": "256MB",
                "spark.sql.adaptive.maxNumPostShufflePartitions": "200",
                "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
                "spark.sql.execution.arrow.pyspark.enabled": "true",
                # Performance optimizations
                "spark.sql.adaptive.autoBroadcastJoinThreshold": "50MB",
                "spark.sql.adaptive.localShuffleReader.enabled": "true",
                "spark.sql.adaptive.skewJoin.enabled": "true",
            }
        }
        
        # Base configuration (always applied)
        base_config = {
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true", 
            "spark.sql.ansi.enabled": "true",
            "spark.jars": "/opt/spark/jars/postgresql.jar",
        }
        
        # Merge base with tier-specific config
        final_config = {**base_config, **configs[tier]}
        
        return final_config
    
    def get_session_builder(self, app_name: str, tier: Optional[str] = None):
        """Get pre-configured SparkSession.builder"""
        from pyspark.sql import SparkSession
        
        config = self.get_config(tier)
        builder = SparkSession.builder.appName(app_name)
        
        for key, value in config.items():
            builder = builder.config(key, value)
        
        # Set master for local development
        builder = builder.master("local[*]")
        
        return builder


# Convenience functions for DAGs
def get_spark_config(tier: Optional[str] = None) -> Dict[str, str]:
    """Get Spark configuration dictionary"""
    generator = SparkConfigGenerator()
    return generator.get_config(tier)


def get_spark_session(app_name: str, tier: Optional[str] = None):
    """Get pre-configured SparkSession"""
    generator = SparkConfigGenerator()
    return generator.get_session_builder(app_name, tier).getOrCreate()


def get_current_tier() -> str:
    """Get currently detected performance tier"""
    generator = SparkConfigGenerator()
    return generator.detect_performance_tier()


if __name__ == "__main__":
    # Demo/testing
    generator = SparkConfigGenerator()
    
    print(f"Detected system: {generator.total_memory_gb}GB RAM, {generator.cpu_count} cores")
    print(f"Recommended tier: {generator.detect_performance_tier()}")
    print()
    
    for tier in ["lite", "balanced", "performance"]:
        print(f"{tier.upper()} Configuration:")
        config = generator.get_config(tier)
        for key, value in config.items():
            print(f"  {key}: {value}")
        print()