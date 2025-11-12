#https://docs.microsoft.com/en-us/powershell/module/microsoft.powershell.diagnostics/get-counter?view=powershell-7.2

#Get current date
$currentdate = Get-Date -Format "yyy-MM-dd"

#Specify file path
$filepath = "C:\counterlogs\network-counter-logs-$currentdate.csv"

Test-Connection -ComputerName CIBL265723, www.google.com -BufferSize 128 | 
Select-Object @{n = 'TimeStamp'; e = { Get-Date  -Format "yyy-MM-dd HH:mm:ss" } }, __SERVER, Address, ProtocolAddress, IPV4Address, IPV6Address, BufferSize, ResponseTime | 
Export-Csv $filepath -Append -Force -NoTypeInformation
