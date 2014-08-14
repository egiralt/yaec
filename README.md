
Yet Another ElasticSearch PHP Client 
====

## Otro cliente de ElasticSearch! para PHP

Como dice su nombre, es un cliente de ElasticSearch.. ni peor ni mejor que los tantos que ya hay. Se ha diseñado para que sea muy simple, usando los constructores básicos de PHP y por tanto alejados de clientes con construcciones más complejas.

Por ahora está en desarrollo, aunque se está usando ya en proyectos personales. Simplemente no ha sido probado en un entorno más "sociable"... entiéndase: no se ha revisado ni criticado mucho. Espero forks!

## Yaec es Orientado a Objetos 
Aplica las mejores prácticas de la OOP para lograr un framework lo más sólido y extensible posible. Es un proyecto que crece día a día, así que requiere de buen código, legible y organizado.

## Cómo se usa

Hagamos unos simples ejemplos:

```
use \Yaec\Yaec_ESClient;

	...
// Conectar al server local para interrogar el índice 'twitter'
$es = new Yaec_ESClient ('twitter', 'localhost', 9200);   

// Ahora a buscar un nuevo objeto, usando su id
$atweet = $es->GetItem('tweet', 1);

// Recuperar los tweets de determinado usuario
$tweets = $es->MatchMany ('tweet', array ('user' => '@johndoe'));

$today = new \DateTime();
$lastTweet = $es->MatchOne ('tweet',array(
	'user' => '@johndoe',
	'posting_date' => $today->format('Y-m-d')
	));
	
echo $lastTweet->message;  // El resultado es una clase PHP 
	
	...

```

## Licencia
GPL

