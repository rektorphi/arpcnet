$version = git describe --tags
$version = $version.Substring(1)

Write-Output "Building Version $version"

$validVersion = $version -match '^[0-9]+.[0-9]+.[0-9]+$'

$versionFile = "./build.go"

try {
    $env:goos = "linux"
    ((Get-Content -path $versionFile -Raw) -replace '0.0.0',$version) | Set-Content -Path $versionFile
    go build ./cmd/arpcnet
}
finally {
    $env:goos = "windows"
    git restore $versionFile
}
Move-Item -Force ./arpcnet ./docker
if ($validVersion) {
    docker build ./docker -t rektorphi/arpcnet:latest
    docker tag rektorphi/arpcnet:latest rektorphi/arpcnet:$version
} else {
    docker build ./docker -t rektorphi/arpcnet:dev
}
Remove-Item ./docker/arpcnet