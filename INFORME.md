# TP COORDINACIÓN

Nombre y Apellido: Facundo Anagua Rocabado

Padrón: 109641

## IDENTIFIACION DE CADA CLIENTE

Para idententificar a cada cliente se le asigna un `query_id` que es único, dicho id es asignado es su instancia de `MessageHandler`. La `query_id` se usa en todos los protocolos internos, esto nos ayuda a que cada SUM, AGGREGATION y JOIN pueda identificar respectivamente a cada cliente sin mezaclar la información.

## CORRDINACION DE LAS INSTANCIAS DE SUM

Dentro de cada instancia de SUM se tienen 2 hilos:

*   Un hilo se encarga de escuchar la cola por EL cual envía mensajes el gateway.
*   El otro hilo se encarga de escuchar otra cola, que se encarga de la comunicación entre las instancias de SUM (Cada instancia de SUM tiene su propia cola).

Cuando a un SUM le llega el EOF además recibe el **total de los mensajes que se enviaron del cliente**. Una vez recibido el EOF, de una `query_id`, dicho SUM actua como **coordinador** y les pregunta a las otras instancias de SUM con un mensaje tipo `CHECK_EOF_READINESS` a sus respectivas colas. Cuando los SUMS reciben el `CHECK_EOF_READINESS` le envian un mensaje al **coordinador** de tipo `CHECK_EOF_RESPONSE` con los mensajes que procesaron actualmente de dicha `query_id`. Una vez que el **coordinador** recibe todos los `CHECK_EOF_RESPONSE` de las instancias de SUM a las que pregunto, suma los mensajes que proceso cada instancia de SUM de dicha `query_id`, incluyendo el **coordinador** y compara con el total que llego con el EOF: 
* Si es menor, se vuelve a repetir el mismo proceso con `CHECK_EOF_READINESS`. 
* Si es igual, se envia un mensaje de tipo `CHECK_EOF_CONFIRM` de dicha `query_id` a las otras instancias de SUM para que envien las sumas parciales de las frutas a los AGGREGATION (el **coordinador** también envia sus frutas).

## COORDINACION DE LAS INSTANCIAS DE AGGREGATION

Las instancias de SUM para definir a que instancia de AGGREGATION enviar las frutas (se envian mensajes de tipo `RECORD`) se calcula un hash basado en la `query_id` y el `tipo de fruta`. Para el hash se usa el algoritmo CR32 no criptografico (De la libreria `zlib`). Una vez que se termian de enviar todas las frutas que tiene un SUM enviar un mensaje de tipo `EOF` de esa `query_id` a cada AGGREGATION.

Los AGGREGATION van procesando los mensajes de tipo `RECORD`, cuando se termina de recibir los los mensajes de tipo `EOF` calcula el top parcial y lo envia a la instancia de JOIN como un mensaje tipo `PARCIAL_TOP`. 

## JOIN

La instancia de JOIN va procesado los tops parciales de cada `query_id`, cuando cada AGGREGATION termina de enviar su top parcial de una `query_id` se calcula el top final y se envia un mensaje de tipo `TOP` al GATEWAY especificando a que `query_id` pertenece.
