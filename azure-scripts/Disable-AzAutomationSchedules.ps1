# Connect to Azure account
# Connect-AzAccount

# Set the context to the desired subscription
# Set-AzContext -SubscriptionId "your-subscription-id"

# Define the Automation Account and Resource Group
$automationAccountName = "<Automation-Account-Name>"
$resourceGroupName = "<Resource-Group-Name>"

try {
    # Retrieve all schedules in the specified Automation Account
    $schedules = Get-AzAutomationSchedule -ResourceGroupName $resourceGroupName -AutomationAccountName $automationAccountName 

    # Loop through each schedule and disable it
    foreach ($schedule in $schedules) {
        Write-Output "Disabling schedule: $($schedule.Name)"
        Set-AzAutomationSchedule -AutomationAccountName $automationAccountName -Name $schedule.Name -IsEnabled $false -ResourceGroupName $resourceGroupName
    }
    Write-Output "All schedules have been disabled."
} catch {
    Write-Error "An error occurred: $_"
}