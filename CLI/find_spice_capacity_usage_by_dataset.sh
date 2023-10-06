#!/bin/bash

# Set your AWS Account ID and AWS CLI profile name
AWS_ACCOUNT_ID="your AWS_ACCOUNT_ID"

# List all datasets and store them in a variable as JSON
datasets_json=$(aws quicksight list-data-sets --aws-account-id $AWS_ACCOUNT_ID)

# Extract dataset IDs and their ConsumedSpiceCapacityInBytes
dataset_ids=$(echo $datasets_json | jq -r '.DataSetSummaries[].DataSetId')

# Loop through dataset IDs and describe each dataset using a while loop
echo "$dataset_ids" | while read -r dataset_id
do
  # Describe the dataset and extract ConsumedSpiceCapacityInBytes
  dataset_info=$(aws quicksight describe-data-set --aws-account-id $AWS_ACCOUNT_ID --data-set-id $dataset_id)

  consumed_spice_capacity=$(echo $dataset_info | jq -r '.DataSet.ConsumedSpiceCapacityInBytes')

  # Print dataset ID and ConsumedSpiceCapacityInBytes
  echo "Dataset ID: $dataset_id , ConsumedSpiceCapacityInBytes: $consumed_spice_capacity"
  echo "-----------------------------"
done
