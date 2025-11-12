<#
.SYNOPSIS
  One way sync of Microsoft Public Azure network addresses and custom NAT addresses into Azure Analysis services firewall.

.DESCRIPTION
  This script will create/update the Azure Analysis Services firewall allow list with the Microsoft Public IPs from Azure Cloud Europe West (VM/Compute IP address range), pulling the source IP ranges from the list of networks Microsoft publish.
  This script adds custom Firewall NAT addresses to the Azure Analysis Services Firewall rules.
  This script is DESTRUCTIVE - it will clear all existing IP rules from targeted Azure Analysis Services Firewall before applying rules from this file.

.LINK
  https://www.microsoft.com/en-us/download/confirmation.aspx?id=56519

.EXAMPLE  
  .\Create-AzFirewallRule.ps1
#>

$startTime = Get-Date
Write-Output "Current time is $startTime"

try {
    # Variables for Azure Runbooks
    # TODO Update
    $EnvironmentName = "Environment-Name"
    $ResourceGroup = Get-AutomationVariable -Name "Resource-Group"
    $ServicePrinciple = Get-AutomationPSCredential -Name "service_principal"
    $TenantId = Get-AutomationVariable -Name "Tenant-Id"
    $VDCSubscription = Get-AutomationVariable -Name "Subscription-Name"

    # Assign account details for Azure Runbook
    Add-AZAccount -Credential $ServicePrinciple -Tenant $TenantId -ServicePrincipal -Subscription $VDCSubscription

    function Get-IPrangeStartEnd {
        <#
        .SYNOPSIS
          Get the IP addresses in a range from CIDR formatted address (e.g. 192.168.0.1/16)
        .EXAMPLE
          Get-IPrangeStartEnd -ip 192.168.8.3 -cidr 24
        #>
        param (
            [string]$ip,
            [int]$cidr
        )
        
        function IPaddresstoINT64 () {  
            param ($ip)  
            $octets = $ip.split(".")  
            return [int64]([int64]$octets[0]*16777216 +[int64]$octets[1]*65536 +[int64]$octets[2]*256 +[int64]$octets[3])  
        }  
          
        function INT64toIPaddress() {  
            param ([int64]$int)  
            return (([math]::truncate($int/16777216)).tostring()+"."+([math]::truncate(($int%16777216)/65536)).tostring()+"."+([math]::truncate(($int%65536)/256)).tostring()+"."+([math]::truncate($int%256)).tostring() ) 
        }  
          
        if ($ip) {$ipaddr = [Net.IPAddress]::Parse($ip)}  
        if ($cidr) {$maskaddr = [Net.IPAddress]::Parse((INT64toIPaddress -int ([convert]::ToInt64(("1"*$cidr+"0"*(32-$cidr)),2)))) }  
        if ($ip) {$networkaddr = new-object net.ipaddress ($maskaddr.address -band $ipaddr.address)}  
        if ($ip) {$broadcastaddr = new-object net.ipaddress (([system.net.ipaddress]::parse("255.255.255.255").address -bxor $maskaddr.address -bor $networkaddr.address))}  
          
        if ($ip) {  
            $startaddr = IPaddresstoINT64 -ip $networkaddr.ipaddresstostring  
            $endaddr = IPaddresstoINT64 -ip $broadcastaddr.ipaddresstostring  
        } else {  
            $startaddr = IPaddresstoINT64 -ip $start  
            $endaddr = IPaddresstoINT64 -ip $end  
        }  
          
        $temp=""| Select-Object start,end 
        $temp.start=INT64toIPaddress -int $startaddr 
        $temp.end=INT64toIPaddress -int $endaddr 
        return $temp
    }

    # Azure IP Ranges and Service Tags – Public Cloud - Download location:
    $downloadUri = "https://www.microsoft.com/en-us/download/confirmation.aspx?id=56519"
    $jsonFileUri = (Invoke-WebRequest -UseBasicParsing $downloadUri).Links.Href | Where-Object {$_  -like "*ServiceTags_Public*.json"}

    # Call JSON link
    $response = Invoke-WebRequest -UseBasicParsing $jsonFileUri[0]

    # Read Web response and convert the required JSON text for Azure Cloud - West Europe
    $jsonResponse = [System.Text.Encoding]::UTF8.GetString($response.Content) | ConvertFrom-Json
    $jsonData = ($jsonResponse.values) | Where-Object { $_.name -eq "AzureCloud.westeurope" } | Select-Object -ExpandProperty "properties"
    $ipAddressList = ($jsonData)  | Select-Object -ExpandProperty "addressPrefixes"

    # Create Empty list to hold IP Rule Object
    $FirewallList = @()
    # Loop through Microsoft IP address list, add name and IP addresses to AZ Firewall rule list
    foreach ($Row in $ipAddressList) {
        # Create Firewall Rule name
        $aasFirewallRuleName = "CustomAzureCompute$($Row -replace "/","-" -replace "\.","_")"
        # Get IP and CIDR from JSON list
        $ipAddressCIDR = $Row -split "/"
        # Run IP Start/End Function using above variable
        $ruleIP = Get-IPrangeStartEnd -ip $ipAddressCIDR[0] -cidr $ipAddressCIDR[1]
        Write-Output "Adding IP Range to Firewall List --> $aasFirewallRuleName; $ruleIP..."

        # Generate Azure Analysis services Firewall rule and add Rule to array
        New-Variable -Name "$($aasFirewallRuleName)" -Value (New-AZAnalysisServicesFirewallRule -FirewallRuleName $aasFirewallRuleName -RangeStart $ruleIP.start -RangeEnd $ruleIP.end)
        $FirewallList += $((Get-Variable -Name "$($aasFirewallRuleName)").Value)
        if("$($aasFirewallRuleName)" ) {Remove-Variable "$($aasFirewallRuleName)" }
    }

    # Add custom NAT IP Addresses to Firewall Rule List
    # TODO: Add ip ranges here
    Write-Output "Adding Internal IP Ranges to Firewall List..."
    $FirewallList += New-AZAnalysisServicesFirewallRule -FirewallRuleName "CustomInternal??" -RangeStart "?.?.?.?" -RangeEnd "?.?.?.?"
    $FirewallList += New-AZAnalysisServicesFirewallRule -FirewallRuleName "CustomInternal??" -RangeStart "?.?.?.?" -RangeEnd "?.?.?.?"
    $FirewallList += New-AZAnalysisServicesFirewallRule -FirewallRuleName "CustomInternal??" -RangeStart "?.?.?.?" -RangeEnd "?.?.?.?"

  
    # Add AZ Firewall rule list to Configuration file and apply
    $FirewallRuleConfig = New-AZAnalysisServicesFirewallConfig -EnablePowerBIService -FirewallRule $FirewallList
    Write-Output "Applying Firewall Rules..."
    # Apply Configuration rule to Azure Analysis Services
    Set-AZAnalysisServicesServer -Name $EnvironmentName -ResourceGroupName "$($ResourceGroup)-$AZEnvironment" -FirewallConfig $FirewallRuleConfig
    Write-Output "Completed Firewall Update on $ResourceGroup-$AZEnvironment"
} catch {
    Write-Error "An error occurred: $_"
}
