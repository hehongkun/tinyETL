package routers

import (
	beego "github.com/beego/beego/v2/server/web"
	"github.com/beego/beego/v2/server/web/context/param"
)

func init() {

    beego.GlobalControllerRouter["tinyETL/controllers:TaskDataController"] = append(beego.GlobalControllerRouter["tinyETL/controllers:TaskDataController"],
        beego.ControllerComments{
            Method: "Post",
            Router: "/add",
            AllowHTTPMethods: []string{"post"},
            MethodParams: param.Make(),
            Filters: nil,
            Params: nil})

    beego.GlobalControllerRouter["tinyETL/controllers:TaskDataController"] = append(beego.GlobalControllerRouter["tinyETL/controllers:TaskDataController"],
        beego.ControllerComments{
            Method: "Delete",
            Router: "/deletetask/:id",
            AllowHTTPMethods: []string{"delete"},
            MethodParams: param.Make(),
            Filters: nil,
            Params: nil})

    beego.GlobalControllerRouter["tinyETL/controllers:TaskDataController"] = append(beego.GlobalControllerRouter["tinyETL/controllers:TaskDataController"],
        beego.ControllerComments{
            Method: "Put",
            Router: "/edittask:id",
            AllowHTTPMethods: []string{"put"},
            MethodParams: param.Make(),
            Filters: nil,
            Params: nil})

    beego.GlobalControllerRouter["tinyETL/controllers:TaskDataController"] = append(beego.GlobalControllerRouter["tinyETL/controllers:TaskDataController"],
        beego.ControllerComments{
            Method: "GetAll",
            Router: "/getall",
            AllowHTTPMethods: []string{"get"},
            MethodParams: param.Make(),
            Filters: nil,
            Params: nil})

    beego.GlobalControllerRouter["tinyETL/controllers:TaskDataController"] = append(beego.GlobalControllerRouter["tinyETL/controllers:TaskDataController"],
        beego.ControllerComments{
            Method: "GetAllTaskList",
            Router: "/getalltasklist",
            AllowHTTPMethods: []string{"get"},
            MethodParams: param.Make(),
            Filters: nil,
            Params: nil})

    beego.GlobalControllerRouter["tinyETL/controllers:TaskDataController"] = append(beego.GlobalControllerRouter["tinyETL/controllers:TaskDataController"],
        beego.ControllerComments{
            Method: "GetOne",
            Router: "/gettask:id",
            AllowHTTPMethods: []string{"get"},
            MethodParams: param.Make(),
            Filters: nil,
            Params: nil})

    beego.GlobalControllerRouter["tinyETL/controllers:UserController"] = append(beego.GlobalControllerRouter["tinyETL/controllers:UserController"],
        beego.ControllerComments{
            Method: "AddUser",
            Router: "/adduser",
            AllowHTTPMethods: []string{"post"},
            MethodParams: param.Make(),
            Filters: nil,
            Params: nil})

    beego.GlobalControllerRouter["tinyETL/controllers:UserController"] = append(beego.GlobalControllerRouter["tinyETL/controllers:UserController"],
        beego.ControllerComments{
            Method: "Delete",
            Router: "/deleteuser:id",
            AllowHTTPMethods: []string{"delete"},
            MethodParams: param.Make(),
            Filters: nil,
            Params: nil})

    beego.GlobalControllerRouter["tinyETL/controllers:UserController"] = append(beego.GlobalControllerRouter["tinyETL/controllers:UserController"],
        beego.ControllerComments{
            Method: "Put",
            Router: "/edituser:id",
            AllowHTTPMethods: []string{"put"},
            MethodParams: param.Make(),
            Filters: nil,
            Params: nil})

    beego.GlobalControllerRouter["tinyETL/controllers:UserController"] = append(beego.GlobalControllerRouter["tinyETL/controllers:UserController"],
        beego.ControllerComments{
            Method: "GetAll",
            Router: "/getall",
            AllowHTTPMethods: []string{"get"},
            MethodParams: param.Make(),
            Filters: nil,
            Params: nil})

    beego.GlobalControllerRouter["tinyETL/controllers:UserController"] = append(beego.GlobalControllerRouter["tinyETL/controllers:UserController"],
        beego.ControllerComments{
            Method: "GetOne",
            Router: "/getuser:id",
            AllowHTTPMethods: []string{"get"},
            MethodParams: param.Make(),
            Filters: nil,
            Params: nil})

    beego.GlobalControllerRouter["tinyETL/controllers:UserController"] = append(beego.GlobalControllerRouter["tinyETL/controllers:UserController"],
        beego.ControllerComments{
            Method: "GetUserFiles",
            Router: "/getuserfiles/:id",
            AllowHTTPMethods: []string{"get"},
            MethodParams: param.Make(),
            Filters: nil,
            Params: nil})

    beego.GlobalControllerRouter["tinyETL/controllers:UserController"] = append(beego.GlobalControllerRouter["tinyETL/controllers:UserController"],
        beego.ControllerComments{
            Method: "Login",
            Router: "/login",
            AllowHTTPMethods: []string{"post"},
            MethodParams: param.Make(),
            Filters: nil,
            Params: nil})

    beego.GlobalControllerRouter["tinyETL/controllers:UserController"] = append(beego.GlobalControllerRouter["tinyETL/controllers:UserController"],
        beego.ControllerComments{
            Method: "RefreshToken",
            Router: "/refreshtoken",
            AllowHTTPMethods: []string{"post"},
            MethodParams: param.Make(),
            Filters: nil,
            Params: nil})

}
