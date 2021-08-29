$timestamp = Get-Date -Format o | ForEach-Object { $_ -replace ":", "." }
$homepath = Resolve-Path ~/keyfreq

mkdir ${homepath}/data
cd $homepath
python "${homepath}/keystats-win.py" ${homepath}/data/keyfreq-${timestamp}.json