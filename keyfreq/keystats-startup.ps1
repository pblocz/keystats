$timestamp = Get-Date -Format o | ForEach-Object { $_ -replace ":", "." }
$homepath = Resolve-Path ~
mkdir ${homepath}/keyfreqs/
cd $homepath
python "${homepath}/keystats-win.py" ${homepath}/keyfreqs/keyfreq-${timestamp}.json