#!/bin/bash
# Pre-ingest all analytics-engine-rest test datasets into a managed OpenSearch cluster.
# This avoids provisioning races when running multiple test classes concurrently.
#
# Usage:
#   export OS_HOST=https://<domain-endpoint>
#   export OS_USER=<username>
#   export OS_PASS=<password>
#   bash ~/pre-ingest-analytics-data.sh

set -uo pipefail

OS_HOST="${OS_HOST:?Set OS_HOST=https://<domain-endpoint>}"
OS_USER="${OS_USER:?Set OS_USER=<username>}"
OS_PASS="${OS_PASS:?Set OS_PASS=<password>}"
AUTH="-u ${OS_USER}:${OS_PASS}"

DATASETS_DIR="/home/bkhishor/OpenSearch/sandbox/qa/analytics-engine-rest/src/test/resources/datasets"

PARQUET_SETTINGS='"index.pluggable.dataformat.enabled": true, "index.pluggable.dataformat": "composite", "index.composite.primary_data_format": "parquet", "index.composite.secondary_data_formats": ["lucene"], '

created=0
failed=0

ingest_index() {
    local index_name="$1"
    local mapping_file="$2"
    local bulk_file="$3"
    local shards="${4:-0}"

    # Skip if index already exists
    if curl -s -o /dev/null -w "%{http_code}" $AUTH "$OS_HOST/$index_name" | grep -q "200"; then
        echo "  ⏭️  $index_name (already exists)"
        return 0
    fi

    # Read mapping and inject parquet settings
    local mapping=$(cat "$mapping_file")
    local body=$(echo "$mapping" | sed "s/\"number_of_shards\"/${PARQUET_SETTINGS}\"number_of_shards\"/")

    # Override shards if requested
    if [ "$shards" -gt 0 ]; then
        body=$(echo "$body" | sed "s/\"number_of_shards\": [0-9]*/\"number_of_shards\": $shards/")
    fi

    # Create index
    local status=$(curl -s -o /tmp/create_resp.json -w "%{http_code}" $AUTH \
        -XPUT "$OS_HOST/$index_name" \
        -H "Content-Type: application/json" \
        -d "$body")

    if [ "$status" != "200" ]; then
        echo "  ❌ $index_name (create failed: $status)"
        cat /tmp/create_resp.json | python3 -c "import sys,json; print('     ', json.load(sys.stdin).get('error',{}).get('reason','unknown')[:100])" 2>/dev/null || true
        failed=$((failed+1))
        return 1
    fi

    # Bulk ingest
    if [ -f "$bulk_file" ] && [ -s "$bulk_file" ]; then
        # Add index action lines if bulk file is just documents
        local bulk_body=$(cat "$bulk_file")
        status=$(curl -s -o /tmp/bulk_resp.json -w "%{http_code}" $AUTH \
            -XPOST "$OS_HOST/$index_name/_bulk?refresh=true" \
            -H "Content-Type: application/x-ndjson" \
            --data-binary "@$bulk_file")

        if [ "$status" != "200" ]; then
            echo "  ⚠️  $index_name (bulk failed: $status)"
            failed=$((failed+1))
            return 1
        fi
    fi

    echo "  ✅ $index_name"
    created=$((created+1))
    return 0
}

echo "=== Pre-ingesting analytics-engine-rest test datasets ==="
echo "Target: $OS_HOST"
echo ""

for dataset_dir in "$DATASETS_DIR"/*/; do
    dataset_name=$(basename "$dataset_dir")
    echo "[$dataset_name]"

    # Single-index dataset: mapping.json + bulk.json → index name = dataset name
    if [ -f "$dataset_dir/mapping.json" ] && [ -f "$dataset_dir/bulk.json" ]; then
        ingest_index "$dataset_name" "$dataset_dir/mapping.json" "$dataset_dir/bulk.json"
    fi

    # Multi-index dataset: mapping_<name>.json + bulk_<name>.json
    for mapping_file in "$dataset_dir"/mapping_*.json; do
        [ -f "$mapping_file" ] || continue
        idx_name=$(basename "$mapping_file" | sed 's/mapping_//;s/\.json//')
        bulk_file="$dataset_dir/bulk_${idx_name}.json"
        if [ -f "$bulk_file" ]; then
            ingest_index "$idx_name" "$mapping_file" "$bulk_file"
        fi
    done
done

# Additional indices used by tests with different names/shards
echo ""
echo "[calcs variants - multi-shard]"
if [ -f "$DATASETS_DIR/calcs/mapping.json" ]; then
    for variant in calcs_a calcs_b calcs_alt calcs_multi_eventstats calcs_multi_sort calcs_multi_streamstats; do
        ingest_index "$variant" "$DATASETS_DIR/calcs/mapping.json" "$DATASETS_DIR/calcs/bulk.json"
    done
fi

echo "[merge_coverage variants]"
if [ -f "$DATASETS_DIR/merge_coverage/mapping.json" ]; then
    ingest_index "merge_coverage_1shard" "$DATASETS_DIR/merge_coverage/mapping.json" "$DATASETS_DIR/merge_coverage/bulk.json"
    ingest_index "merge_coverage_2shard" "$DATASETS_DIR/merge_coverage/mapping.json" "$DATASETS_DIR/merge_coverage/bulk.json" 2
fi

echo "[app_logs variants]"
if [ -f "$DATASETS_DIR/app_logs/mapping.json" ]; then
    ingest_index "app_logs_patterns_multi" "$DATASETS_DIR/app_logs/mapping.json" "$DATASETS_DIR/app_logs/bulk.json" 3
fi

echo "[clickbench - parquet_hits]"
if [ -f "$DATASETS_DIR/clickbench/mapping.json" ]; then
    ingest_index "parquet_hits" "$DATASETS_DIR/clickbench/mapping.json" "$DATASETS_DIR/clickbench/bulk.json" 2
fi

echo "[ip_multishard - 2 shards]"
if [ -f "$DATASETS_DIR/ip_multishard/mapping.json" ]; then
    ingest_index "ip_multishard" "$DATASETS_DIR/ip_multishard/mapping.json" "$DATASETS_DIR/ip_multishard/bulk.json" 2
fi

echo ""
echo "=== Fixing index name mismatches ==="
# Some datasets use a different index name than the dataset directory name
ingest_index "aggregation_tests" "$DATASETS_DIR/aggregations/mapping.json" "$DATASETS_DIR/aggregations/bulk.json"
ingest_index "function_tests" "$DATASETS_DIR/functions/mapping.json" "$DATASETS_DIR/functions/bulk.json"
ingest_index "regex_logs" "$DATASETS_DIR/complex_regex/mapping.json" "$DATASETS_DIR/complex_regex/bulk.json"

echo ""
echo "=== Fixing overlapping datasets ==="
# event_processor is shared between complex_joins and multi_source_joins (superset mapping needed)
echo -n "  event_processor (superset): "
curl -s -o /dev/null $AUTH -XDELETE "$OS_HOST/event_processor"
local_mapping=$(cat "$DATASETS_DIR/multi_source_joins/mapping_event_processor.json")
local_body=$(echo "$local_mapping" | sed "s/\"number_of_shards\"/${PARQUET_SETTINGS}\"number_of_shards\"/")
curl -s -o /dev/null $AUTH -XPUT "$OS_HOST/event_processor" -H "Content-Type: application/json" -d "$local_body"
curl -s -o /dev/null $AUTH -XPOST "$OS_HOST/event_processor/_bulk?refresh=true" -H "Content-Type: application/x-ndjson" --data-binary "@$DATASETS_DIR/multi_source_joins/bulk_event_processor.json"
curl -s -o /dev/null $AUTH -XPOST "$OS_HOST/event_processor/_bulk?refresh=true" -H "Content-Type: application/x-ndjson" --data-binary "@$DATASETS_DIR/complex_joins/bulk_event_processor.json"
echo "✅"

# These indices are shared across datasets - use the dedicated dataset mapping
for idx in kubernetes_logs performance_metrics security_logs; do
  echo -n "  $idx (dedicated): "
  curl -s -o /dev/null $AUTH -XDELETE "$OS_HOST/$idx"
  local_mapping=$(cat "$DATASETS_DIR/$idx/mapping.json")
  local_body=$(echo "$local_mapping" | sed "s/\"number_of_shards\"/${PARQUET_SETTINGS}\"number_of_shards\"/")
  curl -s -o /dev/null $AUTH -XPUT "$OS_HOST/$idx" -H "Content-Type: application/json" -d "$local_body"
  curl -s -o /dev/null $AUTH -XPOST "$OS_HOST/$idx/_bulk?refresh=true" -H "Content-Type: application/x-ndjson" --data-binary "@$DATASETS_DIR/$idx/bulk.json"
  echo "✅"
done

echo ""
echo "=== Done ==="
echo "Created: $created"
echo "Failed: $failed"
echo ""
echo "Verify: curl $AUTH '$OS_HOST/_cat/indices?v&h=index,health,docs.count'"
