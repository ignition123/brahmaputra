package Utilization

import (
	"fmt"
	"github.com/shirou/gopsutil/cpu"
	"github.com/shirou/gopsutil/disk"
	"github.com/shirou/gopsutil/host"
	"github.com/shirou/gopsutil/mem"
	"github.com/shirou/gopsutil/net"
	"runtime"
	"strconv"
    "ChannelList"
)

func dealwithErr(err error) {
	if err != nil {
	    fmt.Println(err)
	}
}

 func GetHardwareData() {

 	defer ChannelList.Recover()

    runtimeOS := runtime.GOOS
    // memory
    vmStat, err := mem.VirtualMemory()
    dealwithErr(err)

	 // disk - start from "/" mount point for Linux
	 // might have to change for Windows!!
	 // don't have a Window to test this out, if detect OS == windows
	 // then use "\" instead of "/"

    diskStat, err := disk.Usage("/")
    dealwithErr(err)

    // cpu - get CPU number of cores and speed
    cpuStat, err := cpu.Info()
    dealwithErr(err)
    percentage, err := cpu.Percent(0, true)
    dealwithErr(err)

    // host or machine kernel, uptime, platform Info
    hostStat, err := host.Info()
    dealwithErr(err)

    // get interfaces MAC/hardware address
    interfStat, err := net.Interfaces()
    dealwithErr(err)

    printDump := "OS : " + runtimeOS + "\r\n"
    printDump = printDump + "Total memory: " + strconv.FormatUint(vmStat.Total, 10) + " bytes \r\n"
    printDump = printDump + "Free memory: " + strconv.FormatUint(vmStat.Free, 10) + " bytes\r\n"
    printDump = printDump + "Percentage used memory: " + strconv.FormatFloat(vmStat.UsedPercent, 'f', 2, 64) + "%\r\n"

    // get disk serial number.... strange... not available from disk package at compile time
    // undefined: disk.GetDiskSerialNumber
    //serial := disk.GetDiskSerialNumber("/dev/sda")

    //printDump = printDump + "Disk serial number: " + serial + "\r\n"

    printDump = printDump + "Total disk space: " + strconv.FormatUint(diskStat.Total, 10) + " bytes \r\n"
    printDump = printDump + "Used disk space: " + strconv.FormatUint(diskStat.Used, 10) + " bytes\r\n"
    printDump = printDump + "Free disk space: " + strconv.FormatUint(diskStat.Free, 10) + " bytes\r\n"
    printDump = printDump + "Percentage disk space usage: " + strconv.FormatFloat(diskStat.UsedPercent, 'f', 2, 64) + "%\r\n"

    // since my machine has one CPU, I'll use the 0 index
    // if your machine has more than 1 CPU, use the correct index
    // to get the proper data

    if len(cpuStat) > 0{
    	printDump = printDump + "CPU index number: " + strconv.FormatInt(int64(cpuStat[0].CPU), 10) + "\r\n"
	    printDump = printDump + "VendorID: " + cpuStat[0].VendorID + "\r\n"
	    printDump = printDump + "Family: " + cpuStat[0].Family + "\r\n"
	    printDump = printDump + "Number of cores: " + strconv.FormatInt(int64(cpuStat[0].Cores), 10) + "\r\n"
	    printDump = printDump + "Model Name: " + cpuStat[0].ModelName + "\r\n"
	    printDump = printDump + "Speed: " + strconv.FormatFloat(cpuStat[0].Mhz, 'f', 2, 64) + " MHz \r\n"
    }
    

    for idx, cpupercent := range percentage {
        printDump = printDump + "Current CPU utilization: [" + strconv.Itoa(idx) + "] " + strconv.FormatFloat(cpupercent, 'f', 2, 64) + "%\r\n"
    }

    printDump = printDump + "Hostname: " + hostStat.Hostname + "\r\n"
    printDump = printDump + "Uptime: " + strconv.FormatUint(hostStat.Uptime, 10) + "\r\n"
    printDump = printDump + "Number of processes running: " + strconv.FormatUint(hostStat.Procs, 10) + "\r\n"

    // another way to get the operating system name
    // both darwin for Mac OSX, For Linux, can be ubuntu as platform
    // and linux for OS

    printDump = printDump + "OS: " + hostStat.OS + "\r\n"
    printDump = printDump + "Platform: " + hostStat.Platform + "\r\n"

    // the unique hardware id for this machine
    printDump = printDump + "Host ID(uuid): " + hostStat.HostID + "\r\n"

    for _, interf := range interfStat {
        printDump = printDump + "------------------------------------------------------\r\n"
        printDump = printDump + "Interface Name: " + interf.Name + "\r\n"

        if interf.HardwareAddr != "" {
            printDump = printDump + "Hardware(MAC) Address: " + interf.HardwareAddr + "\r\n"
        }

        for _, flag := range interf.Flags {
            printDump = printDump + "Interface behavior or flags: " + flag + "\r\n"
        }

        for _, addr := range interf.Addrs {
            printDump = printDump + "IPv6 or IPv4 addresses: " + addr.String() + "\r\n"
        }
    }

    fmt.Println(printDump)

}
