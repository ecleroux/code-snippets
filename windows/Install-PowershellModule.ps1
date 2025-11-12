# Define parameters
param (
    [Parameter(Mandatory=$true)]
    [string]$ModuleName
)

# Check if the module is available
if (!(Get-Module -ListAvailable -Name $ModuleName)) {
    # Install the NuGet package provider if not already installed
    Install-PackageProvider nuget -Force
    
    # Install the specified module
    Install-Module $ModuleName -Force
}