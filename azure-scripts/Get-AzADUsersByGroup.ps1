##############################################################################################
# Azure Active Directory Group and User Management Script
##############################################################################################
# Ensure the required modules are installed
# Install-Module -Name MSOnline
# Install-Module -Name AzureAD

# Connect to Microsoft Online Service
try {
    Connect-MsolService
    Write-Output "Connected to MSOnline service."
} catch {
    Write-Error "Failed to connect to MSOnline service: $_"
}

# Check if AD group exists and get details
$groupName = "your-group-name@yourdomain.onmicrosoft.com"
try {
    $group = Get-MsolGroup -SearchString $groupName
    if ($group) {
        Write-Output "Group found: $($group.DisplayName)"
        $groupId = $group.ObjectId
    } else {
        Write-Output "Group not found: $groupName"
    }
} catch {
    Write-Error "Failed to get group details: $_"
}

# Get list of users belonging to the group
if ($groupId) {
    try {
        $groupMembers = Get-MsolGroupMember -GroupObjectId $groupId
        Write-Output "Members of group $groupName:"
        $groupMembers | ForEach-Object { Write-Output $_.EmailAddress }
    } catch {
        Write-Error "Failed to get group members: $_"
    }
}