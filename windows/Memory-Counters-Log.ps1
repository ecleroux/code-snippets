#https://docs.microsoft.com/en-us/powershell/module/microsoft.powershell.diagnostics/get-counter?view=powershell-7.2

#Get current date
$currentdate = Get-Date -Format "yyy-MM-dd"

#Specify file path
$filepath = "C:\counterlogs\memory-counter-logs-$currentdate.csv"

#Timestamp
$timestamp = Get-Date -Format "yyy-MM-dd HH:mm:ss"

#Log Memory Counters
$counters = (Get-Counter -ListSet Memory).Paths

Get-Counter $counters | Select-Object -expandProperty CounterSamples | group InstanceName | foreach {
    $ht = New-Object System.Collections.Specialized.OrderedDictionary
    $ht.Add("Timestamp", $timestamp)
    foreach ($item in $_.Group) {
        $perfCName = $item.Path.Replace(("(" + $item.InstanceName + ")"), "").Split("\")[3, 4] -join "\"
        $ht.Add($perfCName, $item.CookedValue)
    }
    New-Object PSObject -Property $ht
} | Export-Csv $filepath -Append -Force -NoTypeInformation
