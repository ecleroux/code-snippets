############################################################################################
# Azure
############################################################################################

###########################################################
# Get list of groups a User belongs to
# Uses the AzureAD library
###########################################################

# Ensure the AzureAD module is installed
# Install-Module -Name AzureAD

# Define parameters
param (
    [Parameter(Mandatory=$true)]
    [string]$UserPrincipalName
)

# Connect to Azure AD
try {
    Connect-AzureAD -ErrorAction Stop
    Write-Host "Connected to Azure AD successfully."
} catch {
    Write-Error "Failed to connect to Azure AD. Please check your credentials and network connection."
    exit 1
}

# Get User Details
try {
    $user = Get-AzureADUser -ObjectId $UserPrincipalName -ErrorAction Stop
    Write-Host "User details retrieved successfully."
} catch {
    Write-Error "Failed to retrieve user details. Please check the UserPrincipalName."
    exit 1
}

# Get list of groups user belongs to using the ObjectId from user details
try {
    $groups = Get-AzureADUserMembership -ObjectId $user.ObjectId -ErrorAction Stop
    Write-Host "User groups retrieved successfully."
    $groups | ForEach-Object { Write-Output $_.DisplayName }
} catch {
    Write-Error "Failed to retrieve user groups. Please check the UserPrincipalName."
    exit 1
}
