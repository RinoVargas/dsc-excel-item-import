# dsc-bulk-item-import
Script for importing items into DSpace-CRIS from Excel files using DBMS Import framework

DSpace-CRIS allows the import of CRIS objects using [XLS Import and Export tool](https://wiki.duraspace.org/display/DSPACECRIS/XLS+Import+and+Export+tool). Preparing information in XLS files is fairly straightforward, and allows for ingesting not only plain metadadata values but also pointers to other entities. This tool, however, does not work for native DSpace items.

There are different tools for importing native DSpace items, but in a DSpace-CRIS setting, it usually required to import values pointing to CRIS objects.

CRIS objects are exposed to native DSpace items as authorities, but native [Batch Metadata Editing](https://wiki.duraspace.org/display/DSDOC5x/Batch+Metadata+Editing) does not allow the ingest of pointers to CRIS objects.

One way of importing DSpace items with pointers to CRIS objects is to use the [Simple Archive Format](https://wiki.duraspace.org/display/DSDOC5x/Importing+and+Exporting+Items+via+Simple+Archive+Format) and include authority values inside dublin_core.xml files:
```xml
<dcvalue element="contributor" qualifier="author" authority="rp00199" confidence="600">Lazaro Cubas, Michael</dcvalue>
```

Another way is to utilize [DBMS Import framework](https://wiki.duraspace.org/display/DSPACECRIS/DBMS+Import+framework) functionality (introduced in DSpace-CRIS 5.6.0), which requires the staging of import data in adhoc simplified database tables. 

This repository includes a Python script to help the staging of import data for DBMS Import framework from Excel files similar to those used for [XLS Import and Export tool](https://wiki.duraspace.org/display/DSPACECRIS/XLS+Import+and+Export+tool). 
