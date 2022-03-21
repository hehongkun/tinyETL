package utils

import (
	"fmt"
	"time"
	"tinyETL/models"

	"github.com/dgrijalva/jwt-go"
)

const (
	KEY string = "JWT-ARY-STARK"
	DEFAULT_EXPIRE_SECONDS int = 600 // default 10 minutes

)


// JWT -- json web token
// HEADER PAYLOAD SIGNATURE
// This struct is the PAYLOAD
type MyCustomClaims struct {
	models.User
	jwt.StandardClaims
}


// update expireAt and return a new token
func RefreshToken(tokenString string)(string, error) {
	// first get previous token
	token, err := jwt.ParseWithClaims(
		tokenString,
		&MyCustomClaims{},
		func(token *jwt.Token)(interface{}, error) {
			return []byte(KEY), nil
		})
	claims, ok := token.Claims.(*MyCustomClaims)
	if !ok || !token.Valid {
		return "", err
	}
	mySigningKey := []byte(KEY)
	expireAt  := time.Now().Add(time.Second * time.Duration(DEFAULT_EXPIRE_SECONDS)).Unix()
	newClaims := MyCustomClaims{
		claims.User,
		jwt.StandardClaims{
			ExpiresAt: expireAt,
			Issuer:    claims.User.Username,
			IssuedAt:  time.Now().Unix(),
		},
	}
	// generate new token with new claims
	newToken := jwt.NewWithClaims(jwt.SigningMethodHS256, newClaims)
	tokenStr, err := newToken.SignedString(mySigningKey)
	if err != nil {
		fmt.Println("generate new fresh json web token failed !! error :", err)
		return  "" , err
	}
	return tokenStr, err
}


func ValidateToken(tokenString string) error {
	token, err := jwt.ParseWithClaims(
		tokenString,
		&MyCustomClaims{},
		func(token *jwt.Token)(interface{}, error) {
			return []byte(KEY), nil
		})
	if claims, ok := token.Claims.(*MyCustomClaims); ok && token.Valid {
		fmt.Printf("%v %v", claims.User, claims.StandardClaims.ExpiresAt)
		fmt.Println("token will be expired at ", time.Unix(claims.StandardClaims.ExpiresAt, 0))
	} else {
		fmt.Println("validate tokenString failed !!!",err)
		return err
	}
	return nil
}


func GenerateToken(expiredSeconds int,user models.User) (tokenString string,err error) {
	 if expiredSeconds == 0 {
	 	expiredSeconds = DEFAULT_EXPIRE_SECONDS
	 }
	// Create the Claims
	mySigningKey := []byte(KEY)
	expireAt  := time.Now().Add(time.Second * time.Duration(expiredSeconds)).Unix()
	fmt.Println("token will be expired at ", time.Unix(expireAt, 0) )
	claims := MyCustomClaims{
		user,
		jwt.StandardClaims{
			ExpiresAt: expireAt,
			Issuer:    user.Username,
			IssuedAt:  time.Now().Unix(),
		},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tokenStr, err := token.SignedString(mySigningKey)
	if err != nil {
		return "",err
	}
	return tokenStr,nil
}

// return this result to client then all later request should have header "Authorization: Bearer <token> "
func getHeaderTokenValue(tokenString string) string {
	//Authorization: Bearer <token>
	return fmt.Sprintf("Bearer %s", tokenString)
}


