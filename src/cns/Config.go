package cns

var Config = map[string]string{
    "MAXPROCS":"MAX",
    "Mongodb":"mongodb://localhost:27017",
    "Redis":"localhost:6379",
    "StorageType":"local", // local, gcp, azure, aws
    "DirectoryChannelStorage":"/home/vac_storage/channels/",
    "DirectoryUserStorage":"/home/vac_storage/users/",
    "ServeStaticUrl":"https://pounze.com:8000/channels/images?fileName=",
    "ServeDPStaticUrl":"https://pounze.com:8000/users/users_images?fileName=",
}