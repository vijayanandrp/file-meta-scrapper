#  Meta Scrapper #



A Python3 Application for Unix-based Operating Systems

**Note: Metaforge requires at least python version 3.5 to work!**

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



## Running Metaforge ##

1) Place the files you wish to analyze in the **/media** directory

2) Run metaforge.py

<code> python3 metaforge.py </code>


<br>




**Thanks to...**


Exiftool: https://www.sno.phy.queensu.ca/~phil/exiftool/

pyexifinfo: https://pypi.org/project/pyexifinfo/
