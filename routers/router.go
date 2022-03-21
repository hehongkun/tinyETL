// @APIVersion 1.0.0
// @Title beego Test API
// @Description beego has a very cool tools to autogenerate documents for your API
// @Contact astaxie@gmail.com
// @TermsOfServiceUrl http://beego.me/
// @License Apache 2.0
// @LicenseUrl http://www.apache.org/licenses/LICENSE-2.0.html
package routers

import (
	"strings"
	"tinyETL/controllers"

	beego "github.com/beego/beego/v2/server/web"
	"github.com/beego/beego/v2/server/web/context"
)




func init() {
	var FilterToken = func(ctx *context.Context) {
		// logs.Info("current router path is ", ctx.Request.RequestURI)
		if ctx.Request.RequestURI != "/tinyETL/user/login" && ctx.Input.Header("Authorization") == "" {
			ctx.ResponseWriter.WriteHeader(401)
			ctx.ResponseWriter.Write([]byte("no permission"))
		}
		if ctx.Request.RequestURI != "/tinyETL/user/login" && ctx.Input.Header("Authorization") != "" {
			token := ctx.Input.Header("Authorization")
			token = strings.Split(token, "")[1]
			// validate token
			// invoke ValidateToken in utils/token
			// invalid or expired todo res 401
		}
	}

	beego.InsertFilter("/*",beego.BeforeRouter,FilterToken)
	ns := beego.NewNamespace("/tinyETL",

		beego.NSNamespace("/task_data",
			beego.NSInclude(
				&controllers.TaskDataController{},
			),
		),
		beego.NSNamespace("/user",
			beego.NSInclude(
				&controllers.UserController{},
			),
		),
	)
	beego.AddNamespace(ns)
}
