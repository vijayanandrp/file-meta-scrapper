#  Meta Scrapper #



A Python3 Application for Unix-based Operating Systems

**Note: Meta scrapper requires at least python version 3.5 to work!**

<br>

**Supported Filetypes**

dll | docx | doc  |
exe | gif  | html |
jpeg| mkv  | mp3  |
mp4 | odp  | ods  |
odt | pdf  | png  |
pptx| ppt  | svg  |
torrent |wav | xlsx |
xls  |zip |

<br>

## Setup ##

### Install exiftool ###


**Debian-based**

<code>apt install libimage-exiftool-perl</code>

**RHEL-based**

<code>yum install perl-Image-ExifTool</code>

**Arch Linux**

<code>pacman -S perl-image-exiftool</code>

**Mac OSX**

<code>brew install exiftool </code>


### Install dependencies ###

<code> pip3 install -r requirements.txt </code>

<hr>



## Running Meta scrapper ##

1) To scan you can modify the scan_dir value in definitions.py or you can pass the value during runtime

2) Run meta-scrapper.py

<code> python3 meta-scrapper.py </code>


<br>




**Thanks to...**


Exiftool: https://www.sno.phy.queensu.ca/~phil/exiftool/

pyexifinfo: https://pypi.org/project/pyexifinfo/
