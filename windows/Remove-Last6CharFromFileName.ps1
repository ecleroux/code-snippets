<#
.SYNOPSIS
Removes the last 6 characters from all file names in the specified folder. This includes the file extension.

.PARAMETER FolderPath
The path to the folder containing the files to be renamed.

.PARAMETER CharCount
The number of characters to remove from the end of each file name.

.EXAMPLE
.\Remove-Last6CharFromFileName.ps1 -FolderPath 'C:\TestFolder' -CharCount 6
#>

param (
    [string]$FolderPath = 'C:\TestFolder',
    [int]$CharCount = 6
)

# Get all files in the specified folder and rename them
Get-ChildItem -Path $FolderPath | ForEach-Object {
    $newName = $_.Name.Substring(0, $_.Name.Length - $CharCount)
    Rename-Item -Path $_.FullName -NewName $newName
}