/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package sys.bean;

import java.awt.event.ActionEvent;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import javax.faces.application.FacesMessage;
import javax.faces.bean.ManagedBean;
import javax.faces.bean.SessionScoped;

import javax.faces.context.FacesContext;

import javax.servlet.http.HttpSession;
import org.primefaces.context.RequestContext;
import sys.dao.AlmacenDAO;
import sys.dao.AreaDAO;
import sys.dao.CentroCostoDAO;
import sys.dao.CompanyaDAO;
import sys.dao.LocalidadDAO;
import sys.dao.UsuarioDAO;
import sys.imp.AlmacenDAOImp;
import sys.imp.AreaDAOImp;
import sys.imp.CentroCostoDAOImp;
import sys.imp.CompanyaDAOImp;
import sys.imp.LocalidadDAOImp;
import sys.imp.UsuarioDAOImp;
import sys.model.AgmaeArea;
import sys.model.AgmaeCentrocosto;
import sys.model.AgmaeCompanya;
import sys.model.AgmaeEstablecimiento;
import sys.model.AgmaeUsuario;
import sys.model.AimarAlmacen;
import sys.model.AvmovMovNotaDespachoCab;

@ManagedBean
@SessionScoped
public class LoginBean implements Serializable {

    private AgmaeUsuario usuario;
    private AgmaeCompanya companya;
    private List<AgmaeCompanya> listaCompanya;

    private String nombreUsuario;
    private String password;
    private String anyo;
    private Date fechaActual;
    
    //Variables de filtro cabecera

    private List<AgmaeEstablecimiento> listaLocalidad;
    private AgmaeEstablecimiento seleccionEstablecimiento;
    private List<AgmaeArea> listaArea;
    private AgmaeArea seleccionArea;
    private List<AgmaeCentrocosto> listaCentro;
    private AgmaeCentrocosto seleccionCentro;
    private List<AimarAlmacen> listaAlmacen;
    private AimarAlmacen seleccionAlmacen;


    public LoginBean() {
        fechaActual = new Date();
        SimpleDateFormat formatoFecha = new SimpleDateFormat("yyyy");
        this.anyo = formatoFecha.format(fechaActual);
        this.usuario = new AgmaeUsuario();
        this.listaCompanya = new ArrayList<>();
        this.companya = new AgmaeCompanya();

        //Filtros Cabacera
//        
//        usuario.setRucCompanyia("20557265661");
//        usuario.setCodEstablecimiento("0001");
//        usuario.setCodCentroc(5);
//        usuario.setCodArea(3);
//        usuario.setCodAlmacen(1);
//

    }

    public AgmaeUsuario getUsuario() {
        return usuario;
    }

    public void setUsuario(AgmaeUsuario usuario) {
        this.usuario = usuario;
    }

    public AgmaeCompanya getCompanya() {
        return companya;
    }

    public void setCompanya(AgmaeCompanya companya) {
        this.companya = companya;
    }

    public List<AgmaeCompanya> getListaCompanya() {
        CompanyaDAO compDAO = new CompanyaDAOImp();
        setListaCompanya(compDAO.listarCompanyas());
        return listaCompanya;
    }

    public void setListaCompanya(List<AgmaeCompanya> listaCompanya) {
        this.listaCompanya = listaCompanya;
    }

    public String getNombreUsuario() {
        return nombreUsuario;
    }

    public void setNombreUsuario(String nombreUsuario) {
        this.nombreUsuario = nombreUsuario;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getAnyo() {
        return anyo;
    }

    public void setAnyo(String anyo) {
        this.anyo = anyo;
    }

    public Date getFechaActual() {
        return fechaActual;
    }

    public void setFechaActual(Date fechaActual) {
        this.fechaActual = fechaActual;
    }

    
  
    public List<AgmaeEstablecimiento> getListaLocalidad() {
        LocalidadDAO lDao = new LocalidadDAOImp();
        listaLocalidad = lDao.listarEstablecimientos(usuario);
        return listaLocalidad;
    }

    public void setListaLocalidad(List<AgmaeEstablecimiento> listaLocalidad) {
        this.listaLocalidad = listaLocalidad;
    }

    public AgmaeEstablecimiento getSeleccionEstablecimiento() {
        return seleccionEstablecimiento;
    }

    public void setSeleccionEstablecimiento(AgmaeEstablecimiento seleccionEstablecimiento) {
        this.seleccionEstablecimiento = seleccionEstablecimiento;
    }

    public List<AgmaeArea> getListaArea() {
        AreaDAO aDao = new AreaDAOImp();
        listaArea = aDao.listarAreas(usuario);
        return listaArea;
    }

    public void setListaArea(List<AgmaeArea> listaArea) {
        this.listaArea = listaArea;
    }

    public AgmaeArea getSeleccionArea() {
        return seleccionArea;
    }

    public void setSeleccionArea(AgmaeArea seleccionArea) {
        this.seleccionArea = seleccionArea;
    }

    public List<AgmaeCentrocosto> getListaCentro() {
        CentroCostoDAO cCDao = new CentroCostoDAOImp();
        listaCentro = cCDao.listarCentros(usuario);
        return listaCentro;
    }

    public void setListaCentro(List<AgmaeCentrocosto> listaCentro) {
        this.listaCentro = listaCentro;
    }

    public AgmaeCentrocosto getSeleccionCentro() {
        return seleccionCentro;
    }

    public void setSeleccionCentro(AgmaeCentrocosto seleccionCentro) {
        this.seleccionCentro = seleccionCentro;
    }

    public List<AimarAlmacen> getListaAlmacen() {
        AlmacenDAO almDao = new AlmacenDAOImp();
        listaAlmacen = almDao.listarAlmacenes(usuario);
        return listaAlmacen;
    }

    public void setListaAlmacen(List<AimarAlmacen> listaAlmacen) {
        this.listaAlmacen = listaAlmacen;
    }

    public AimarAlmacen getSeleccionAlmacen() {
        return seleccionAlmacen;
    }

    public void setSeleccionAlmacen(AimarAlmacen seleccionAlmacen) {
        this.seleccionAlmacen = seleccionAlmacen;
    }


    public void listoLocalidad() {

        usuario.setCodEstablecimiento(seleccionEstablecimiento.getCodEstablecimiento());
        usuario.setRucCompanyia(seleccionEstablecimiento.getRucCompanyia());

    }

    public void listoArea() {
        usuario.setCodArea(seleccionArea.getCodArea());
        usuario.setAgmaeArea(seleccionArea);

    }

    public void listoCentro() {
        usuario.setCodCentroc(seleccionCentro.getCodCentroc());
        usuario.setAgmaeCentrocosto(seleccionCentro);

    }

    public void listoAlmacen() {
        usuario.setCodAlmacen(seleccionAlmacen.getCodAiAlmacen());
        usuario.setAimarAlmacen(seleccionAlmacen);

    }

    

    public void login(ActionEvent event) {
        RequestContext context = RequestContext.getCurrentInstance();
        FacesMessage message = null;
        boolean loggedIn = false;

        String ruta = "";

        UsuarioDAO uDao = new UsuarioDAOImp();
        this.usuario = uDao.login(this.usuario);

        if (this.usuario != null) {
            FacesContext.getCurrentInstance().getExternalContext().getSessionMap().put("usuario", this.usuario.getLoginUsuario());

            loggedIn = true;
            message = new FacesMessage(FacesMessage.SEVERITY_INFO, "Bienvenid@", usuario.getNomUsuario());
            ruta = "/WEutto/faces/views/modulo.xhtml";
        } else {
            loggedIn = false;
            message = new FacesMessage(FacesMessage.SEVERITY_WARN, "Error de acceso", "Usuario o password es incorrecto");
            this.usuario = new AgmaeUsuario();
        }
        FacesContext.getCurrentInstance().addMessage(null, message);
        context.addCallbackParam("loggedIn", loggedIn);
        context.addCallbackParam("ruta", ruta);

    }

    //Metodo para cerrar la sesion
    public String cerrarSession() {
        this.nombreUsuario = null;
        this.password = null;
        HttpSession httpSession = (HttpSession) FacesContext.getCurrentInstance().getExternalContext().getSession(false);
        httpSession.invalidate();//para borrar la session
        return "/login";
    }

    public void seleccionCompanya() {
        CompanyaDAO compDAO = new CompanyaDAOImp();
        this.companya = compDAO.consultarObjCompanya(companya.getRucCompanya());

    }

}
