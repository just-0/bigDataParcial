#!/bin/bash
echo "=========================================="
echo "COMPARACIÓN JOB 1 (RAW) vs JOB 2 (CLEAN)"
echo "=========================================="
echo ""

echo "Archivos RAW (Job 1):"
aws s3 ls s3://emr-logs-1758750407/music-data/raw-parquet/ \
    --region sa-east-1 --recursive --summarize | grep "Total Size"

echo ""
echo "Archivos CLEAN (Job 2):"
aws s3 ls s3://emr-logs-1758750407/music-data/cleaned/ \
    --region sa-east-1 --recursive --summarize | grep "Total Size"

echo ""
echo "=========================================="
