// @APIVersion 1.0.0
// @Title beego Test API
// @Description beego has a very cool tools to autogenerate documents for your API
// @Contact astaxie@gmail.com
// @TermsOfServiceUrl http://beego.me/
// @License Apache 2.0
// @LicenseUrl http://www.apache.org/licenses/LICENSE-2.0.html
package routers

import (
	"tinyETL/controllers"
	"tinyETL/utils"

	beego "github.com/beego/beego/v2/server/web"
	"github.com/beego/beego/v2/server/web/context"
	"github.com/beego/beego/v2/server/web/filter/cors"
)

func init() {
	var FilterToken = func(ctx *context.Context) {
		// logs.Info("current router path is ", ctx.Request.RequestURI)
		if ctx.Request.RequestURI != "/tinyETL/task/parse" && ctx.Request.RequestURI != "/tinyETL/task/GetNodeInfo" && ctx.Request.RequestURI != "/tinyETL/task/run" && ctx.Request.RequestURI != "/tinyETL/task/runTask" && ctx.Request.RequestURI != "/tinyETL/task/schedule" && ctx.Request.RequestURI != "/tinyETL/componentlog/" && ctx.Request.RequestURI != "/tinyETL/tasklog/" && ctx.Request.RequestURI != "/tinyETL/user/login" && ctx.Request.RequestURI != "/tinyETL/user/refreshtoken" && ctx.Input.Header("Authorization") == "" {
			ctx.ResponseWriter.WriteHeader(401)
			ctx.ResponseWriter.Write([]byte("no permission"))
		}
		if ctx.Request.RequestURI != "/tinyETL/task/parse" && ctx.Request.RequestURI != "/tinyETL/task/run" && ctx.Request.RequestURI != "/tinyETL/task/runTask" && ctx.Request.RequestURI != "/tinyETL/task/schedule" && ctx.Request.RequestURI != "/tinyETL/tasklog/" && ctx.Request.RequestURI != "/tinyETL/user/refreshtoken" && ctx.Input.Header("Authorization") != "" {
			token := ctx.Input.Header("Authorization")
			if _, err := utils.ValidateToken(token); err != nil {
				ctx.ResponseWriter.WriteHeader(401)
				ctx.ResponseWriter.Write([]byte("no permission"))
			}
			// validate token
			// invoke ValidateToken in utils/token
			// invalid or expired todo res 401
		}
	}

	beego.InsertFilter("*", beego.BeforeRouter, cors.Allow(&cors.Options{
		AllowAllOrigins:  true,
		AllowMethods:     []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowHeaders:     []string{"Origin", "Authorization", "Access-Control-Allow-Origin", "Access-Control-Allow-Headers", "Content-Type"},
		ExposeHeaders:    []string{"Content-Length", "Access-Control-Allow-Origin", "Access-Control-Allow-Headers", "Content-Type"},
		AllowCredentials: true,
	}))

	beego.InsertFilter("*", beego.BeforeRouter, FilterToken)
	ns := beego.NewNamespace("/tinyETL",
		beego.NSNamespace("/task",
			beego.NSInclude(
				&controllers.TaskDataController{},
			),
		),
		beego.NSNamespace("/user",
			beego.NSInclude(
				&controllers.UserController{},
			),
		),
		beego.NSNamespace("/tasklog",
			beego.NSInclude(
				&controllers.TaskLogController{},
			),
		),
		beego.NSNamespace("/componentlog",
			beego.NSInclude(
				&controllers.ComponentLogController{},
			),
		),
	)
	beego.AddNamespace(ns)
}
