#  Meta Scrapper #

Created to file forensic analysis based on file meta data (attributes).
Backed provided cassandra support for data storage and analysis.


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


<br>

<hr>

### Install dependencies ###

<code> pip3 install -r requirements.txt </code>

<hr>



## Running Meta scrapper ##

1) To scan you can modify the **scan_dir** value in **definitions.py** or you can pass the value during runtime

2) Run `meta_scrapper.py`  Returns a Excel File

<code> python3 meta_scrapper.py </code>

3) Run `meta_analyse.py`  Returns a Json File

<code> python3 meta_analyse.py </code>


<br>


## CASSANDRA SUPPORT ##
`pip3 install cassandra-driver`


**Thanks to...**


Exiftool: https://www.sno.phy.queensu.ca/~phil/exiftool/

pyexifinfo: https://pypi.org/project/pyexifinfo/
