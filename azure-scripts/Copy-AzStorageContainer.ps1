# Documentation: https://docs.microsoft.com/en-us/azure/storage/common/storage-ref-azcopy-copy

# Function to copy data from one Azure Storage container to another
function Copy-AzStorageContainer {
    param (
        [string]$sourceUrl,
        [string]$destinationUrl
    )
    azcopy copy $sourceUrl $destinationUrl --recursive=true
}

# Example usage of the Copy-AzStorageContainer function
Copy-AzStorageContainer `
    -sourceUrl "https://<source_storage_account>.dfs.core.windows.net/<source_container>?SAS_token_for_container_in_source_storage_account" `
    -destinationUrl "https://<destination_storage_account>.dfs.core.windows.net/<destination_container>?SAS_token_for_container_in_destination_storage_account"
