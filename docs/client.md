# Client example

# Schema declaration

Collection schema is declared using Go struct tags. Special "tigrisdb" tag contains field properties.
The following properties is recognised: 
  * primary_key - declares primary key of the collection
  * pii - Person identifier information
  * encrypted - End-to-end encrypted field

Composite primary keys defined using optional indexes.

Special tag value "-" can be specified to skip field persistence.

Example:

```golang
type CompositePK struct {
    Key1 string `tigrisdb:"primary_key:1"`
    Key2 string `tigrisdb:"primary_key:2"`
	Data1 int
}
```

```golang
package exampleClient

type User struct {
	Email string `json:"email" tigrisdb:"primary_key"`
	Name string `json:"name" tigrisdb:"pii"`
	PaymentInfo []byte `json:"name" tigrisdb:"encrypted"`
}

// Create new client and establish connection to the TigrisDB 
// instance at "localhost:8081"
c, err := client.NewClient(ctx, &config.Config{URL: "localhost:8081"})

// Get database object
db := c.Database("db1")

// Create database
err = db.Create()

// Create or migrate collection schema for User type
err = db.MigrateSchema(ctx, &User{})

// Get collection object for the User collection
coll := db.Collection(&User{})

user1 := &User{Email: "john@example.org", Name: "John Doe"})
user2 := &User{Email: "jane@example.org", Name: "Jane Doe"})

// Insert two documents into the User collection
err = coll.Insert(ctx, user1, user2)

```