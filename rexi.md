java -jar app/build/libs/app.jar -o export -k DBForms.csv -f DBForms.dat     

MIXT0001-mxt-form-db,hash,MRG,MRG,
MIXT0001-mxt-form-db-elements,zindex,MRG,MRG,

MIXT0001-mxt-form-db,hash,MRG,MRG,...base64 string
text_input_36BCCB47-A0B0-4F1F-9B7D-11863DC31728,hash,MRG,MRG,
text_input_56A1C959-74DB-48A7-8CE2-60706DCA85D5,hash,MRG,MRG,
text_input_C7137981-45BA-4556-8588-B65390372F73,hash,MRG,MRG
text_input_DC6765E3-11C8-436E-B748-3693F9A8B2BE,hash,MRG,MRG,
MIXT0001-mxt-form-db-elements,zindex,MRG,MRG,


java -jar rexi.jar -o import -h bit.work -p 6378 -d 0 -k DBForms.csv -f DBForms.dat -a 3fWwBtYy7bx8Ba5bN8p5ERC8A5q0EHFj9ulylbnb5ERC8A5BtYy7bx8WwBtYy7bx8Ba5


> cp ./app/build/libs/app.jar ./rexi.jar 